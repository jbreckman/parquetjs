'use strict';

var counter = 0;

var cache = {}; // TODO: LRU
function memoize(key, fn) {
  return cache[key] || (cache[key] = fn());
}

function parseItem(item) {
  if (item.and) {
    return new AndReader(item.and);
  }
  else if (item.or) {
    return new OrReader(item.or);
  }
  else if (item.min !== undefined || item.max !== undefined) {
    return new RangeReaderItem(item);
  }
  else if (item.value !== undefined) {
    return new ValueReaderItem(item);
  }
  else {
    return new PathReaderItem(item);
  }
}

class MultiReader {
  constructor(paths) {
    this.items = paths.map(parseItem);
  }
  
  prepareColumns(reader) {
    this.reader = reader;
    this.items.forEach(item => item.prepareColumns(reader));
  }

  async loadOffsetInfo(rowGroup, target, cache) {
    if (!target) {
      target = {};
    }
    if (!cache) {
      cache = {};
    }
    await Promise.all(this.items.map(item => item.loadOffsetInfo(rowGroup, target, cache)));
    return target;
  }

  async findRelevantPages(rowGroup) {
    let cache = {};

    let cb = await this.preloadRelevantPageInfo(rowGroup, cache);

    let cacheKeys = Object.keys(cache);
    let someColInfo = await (cacheKeys.length ? cache[cacheKeys[0]] : memoize(`${this.reader.id}_columnIndex0`, () => this.reader.readColumnIndex(rowGroup.columns[0])));

    return someColInfo.max_values.map((d,i)=>i).filter(index => {
      return cb(index);
    });
  }

  async getRecords(offsetInfo, pageNo) {
    let records = [],
        cache = {},
        filteredResults = null;


    records = await this.loadFilteredFields(offsetInfo, pageNo, records, cache);

    // we might not have any filtered fields to load, and if that's the case then
    // don't worry about it.  but if there are filtered records, then we shoul see
    // if we should short circuit
    if (records.length > 0) {
      filteredResults = records.filter(record => this.filterRecord(record));

      // if all records are filtered... we should bail and not load other columns
      if (filteredResults.length === 0) {
        return [];
      }
    }

    await this.loadRecords(offsetInfo, pageNo, records, cache);

    return filteredResults || records;
  }

  async loadRecords(offsetInfo, pageNo, records, cache) {
    await Promise.all(this.items.map(item => item.loadRecords(offsetInfo, pageNo, records, cache)))
    return records;
  }

  async loadFilteredFields(offsetInfo, pageNo, records, cache) {
    await Promise.all(this.items.map(item => item.loadFilteredFields(offsetInfo, pageNo, records, cache)))
    return records;
  }
}

class AndReader extends MultiReader {
  matchesRowGroupStats(rowGroup) {
    return this.items.every(item => item.matchesRowGroupStats(rowGroup));
  }

  filterRecord(record) {
    return this.items.every(item => item.filterRecord(record));
  }

  async preloadRelevantPageInfo(rowGroup, cache) {
    let items = await Promise.all(this.items.map(item => item.preloadRelevantPageInfo(rowGroup, cache)));
    return pageIndex => items.every(item => item(pageIndex));
  }
}

class OrReader extends MultiReader {
  matchesRowGroupStats(rowGroup) {
    return this.items.some(item => item.matchesRowGroupStats(rowGroup));
  }

  filterRecord(record) {
    return this.items.some(item => item.filterRecord(record));
  }

  async preloadRelevantPageInfo(rowGroup, cache) {
    let items = await Promise.all(this.items.map(item => item.preloadRelevantPageInfo(rowGroup, cache)));
    return pageIndex => items.some(item => item(pageIndex));
  }
}

class PathReaderItem {
  constructor(item) {
    this.path = item.path;
    this.index = item.index;
    this.source = item.source;
  }

  prepareColumns(reader) {
    this.reader = reader;
    this.columnIndex = reader.metadata.row_groups[0].columns.findIndex(d => d.meta_data.path_in_schema.join(',') === this.path);
  }

  matchesRowGroupStats(rowGroup) {
    return true;
  }

  filterRecord(record) {
    return true;
  }

  async loadOffsetInfo(rowGroup, target, cache) {
    if (!cache[this.columnIndex]) {
      cache[this.columnIndex] = true;
      const column = rowGroup.columns[this.columnIndex];
      let offsetIndex = await memoize(`${this.reader.id}_offsetIndex${this.columnIndex}`, () => this.reader.readOffsetIndex(column));
      target[this.columnIndex] = offsetIndex;
      target.pageCount = offsetIndex.page_locations.length;
    }
    return target;
  }

  preloadRelevantPageInfo(rowGroup, cache) {
    return () => true;
  }

  async loadFilteredFields(offsetInfo, pageNo, records, cache) {
    if (!this.filtered || this.index) {
      return records;
    }
    return await (cache[this.columnIndex] || (cache[this.columnIndex]=this.reader.readPage(offsetInfo[this.columnIndex], pageNo, records)));
  }

  async loadRecords(offsetInfo, pageNo, records, cache) {
    if (this.index) {
      return records;
    }

    let results = await (cache[this.columnIndex] || (cache[this.columnIndex]=this.reader.readPage(offsetInfo[this.columnIndex], pageNo, records)));

    if (this.source) {
      results.forEach(record => {
        Object.assign(record, JSON.parse(record[this.path]));
        delete record[this.path];
      });
    }

    return results;
  }
}

class RangeReaderItem extends PathReaderItem {
  constructor(item) {
    super(item);
    this.min = item.min;
    this.max = item.max;
    this.filtered = true;
  }

  matchesRowGroupStats(rowGroup) {
    const column = rowGroup.columns[this.columnIndex];
    const stats = column.meta_data.statistics;

    return ((this.min === undefined || stats.max_value >= this.min) && 
            (this.max === undefined || stats.min_value <= this.max));
  }

  filterRecord(record) {
    if (this.index) {
      return true;
    }

    let value = record[this.path];
    if ((this.min !== undefined && value < this.min) ||
        (this.max !== undefined && value > this.max)) {
      return false;
    }
    return true;
  }

  async preloadRelevantPageInfo(rowGroup, cache) {
    let colInfo = await (cache[this.columnIndex] || (cache[this.columnIndex] = 
      memoize(`${this.reader.id}_columnIndex${this.columnIndex}`, 
        () => this.reader.readColumnIndex(rowGroup.columns[this.columnIndex]))));
    
    return pageIndex => {
      if ((this.min !== undefined && this.min > colInfo.max_values[pageIndex]) ||
          (this.max !== undefined && this.max < colInfo.min_values[pageIndex])) {
        return false;
      }
      return true;
    }
  }
}

class ValueReaderItem extends PathReaderItem {
  constructor(item) {
    super(item);
    this.value = item.value;
    this.filtered = true;
  }

  filterRecord(record) {
    if (this.index) {
      return true;
    }

    return record[this.path] == this.value;
  }

  matchesRowGroupStats(rowGroup) {
    const column = rowGroup.columns[this.columnIndex];
    const stats = column.meta_data.statistics;

    return (stats.min_value <= this.value && stats.max_value >= this.value);
  }

  async preloadRelevantPageInfo(rowGroup, cache) {
    let colInfo = await (cache[this.columnIndex] || (cache[this.columnIndex] = 
      memoize(`${this.reader.id}_columnIndex${this.columnIndex}`, 
        () => this.reader.readColumnIndex(rowGroup.columns[this.columnIndex]))));

    return pageIndex => {
      return (colInfo.min_values[pageIndex] <= this.value && colInfo.max_values[pageIndex] >= this.value);
    }
  }
}

class PathReader extends AndReader  {
  constructor(reader, paths) {
    super(paths);
    this.prepareColumns(reader);
  }
}

module.exports = PathReader;