'use strict';
const QuickLRU = require('quick-lru');
const lru = new QuickLRU({maxSize: 1000});

function memoize(key, fn) {
  let result = lru.get(key);
  if (!result) {
    result = fn();
    lru.set(key, result);
  }
  return result;
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

  firstColumnIndex() {
    return this.items[0].firstColumnIndex();
  }

  async loadOffsetInfo(rowGroup, target) {
    if (!target) {
      target = {};
    }
    await Promise.all(this.items.map(item => item.loadOffsetInfo(rowGroup, target)));
    return target;
  }

  async findRelevantPages(rowGroup) {
    let cb = await this.preloadRelevantPageInfo(rowGroup);
    let columnIndex = this.firstColumnIndex();
    let someColInfo = await memoize(`${this.reader.id}_columnIndex${columnIndex}`, () => this.reader.readColumnIndex(rowGroup.columns[0]));
    return someColInfo.max_values.map((d,i)=>i).filter(index => {
      return cb(index);
    });
  }

  async getRecords(offsetInfo, pageNo, rowGroupNo) {
    let records = [],
        filteredResults = null;

    records._columnsLoaded = new Set();
    records = await this.loadFilteredFields(offsetInfo, pageNo, records, rowGroupNo);

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

    await this.loadRecords(offsetInfo, pageNo, records, rowGroupNo);

    return filteredResults || records;
  }

  async loadRecords(offsetInfo, pageNo, records, rowGroupNo) {
    await Promise.all(this.items.map(item => item.loadRecords(offsetInfo, pageNo, records, rowGroupNo)))
    return records;
  }

  async loadFilteredFields(offsetInfo, pageNo, records, rowGroupNo) {
    await Promise.all(this.items.map(item => item.loadFilteredFields(offsetInfo, pageNo, records, rowGroupNo)))
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

  async preloadRelevantPageInfo(rowGroup) {
    let items = await Promise.all(this.items.map(item => item.preloadRelevantPageInfo(rowGroup)));
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

  async preloadRelevantPageInfo(rowGroup) {
    let items = await Promise.all(this.items.map(item => item.preloadRelevantPageInfo(rowGroup)));
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

  firstColumnIndex() {
    return this.columnIndex;
  }

  matchesRowGroupStats(rowGroup) {
    return true;
  }

  filterRecord(record) {
    return true;
  }

  async loadOffsetInfo(rowGroup, target) {  
    const column = rowGroup.columns[this.columnIndex];
    let offsetIndex = await memoize(`${this.reader.id}_offsetIndex${this.columnIndex}`, () => this.reader.readOffsetIndex(column));
    target[this.columnIndex] = offsetIndex;
    target.pageCount = offsetIndex.page_locations.length;
    return target;
  }

  preloadRelevantPageInfo(rowGroup) {
    return () => true;
  }

  async _loadRecords(offsetInfo, pageNo, records, rowGroupNo) {
    if (records._columnsLoaded.has(this.columnIndex)) {
      return;
    }
    records._columnsLoaded.add(this.columnIndex);

    let columnRecords = await memoize(`${this.reader.id}_page_${rowGroupNo}_${pageNo}_${this.source}`, 
        async () => {
          let r = await this.reader.readPage(offsetInfo[this.columnIndex], pageNo, []);
          if (this.source) {
            r = r.map(d => JSON.parse(d[this.path]))
          }
          return r;
        });

    if (records.length === 0) {
      columnRecords.forEach(record => records.push(Object.assign({}, record)));
    }
    else {
      records.map((record, index) => Object.assign(record, columnRecords[index]));
    }
    return records;
  }

  async loadFilteredFields(offsetInfo, pageNo, records, rowGroupNo) {
    if (!this.filtered || this.index) {
      return records;
    }
    return await this._loadRecords(offsetInfo, pageNo, records, rowGroupNo);
  }

  async loadRecords(offsetInfo, pageNo, records, rowGroupNo) {
    if (this.index) {
      return records;
    }
    return await this._loadRecords(offsetInfo, pageNo, records, rowGroupNo);
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

  async preloadRelevantPageInfo(rowGroup) {
    let colInfo = await memoize(`${this.reader.id}_columnIndex${this.columnIndex}`, 
        () => this.reader.readColumnIndex(rowGroup.columns[this.columnIndex]));

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

  async preloadRelevantPageInfo(rowGroup) {
    let colInfo = await memoize(`${this.reader.id}_columnIndex${this.columnIndex}`, 
        () => this.reader.readColumnIndex(rowGroup.columns[this.columnIndex]));

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