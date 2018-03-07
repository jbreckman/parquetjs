'use strict';

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

  async getRecords(offsetInfo, pageNo) {
    let records = [],
        filteredRecordIndices = new Set(),
        cache = {};

    await this.filterRecords(offsetInfo, pageNo, records, filteredRecordIndices, cache);

    // if all records are filtered... we should bail and not load other columns
    if (records.length > 0 && filteredRecordIndices.size === records.length) {
      return [];
    }

    records = await this.loadRecords(offsetInfo, pageNo, records, cache);

    return records.filter((record, index) => {
      return !filteredRecordIndices.has(index);
    });
  }

  async loadRecords(offsetInfo, pageNo, records, cache) {
    await Promise.all(this.items.map(item => item.loadRecords(offsetInfo, pageNo, records, cache)))
    return records;
  }
}

class AndReader extends MultiReader {
  matchesRowGroupStats(rowGroup) {
    return this.items.every(item => item.matchesRowGroupStats(rowGroup));
  }

  async findIgnorePages(rowGroup, ignoredPageSet, cache) {
    if (ignoredPageSet === undefined) {
      ignoredPageSet = new Set();
    }
    if (cache === undefined) {
      cache = {};
    }

    // any item can remove something from the ignored page set
    await Promise.all(this.items.map(item => item.findIgnorePages(rowGroup, ignoredPageSet, cache)));

    return ignoredPageSet;
  }

  async filterRecords(offsetInfo, pageNo, records, filteredRecordIndices, cache) {
    await Promise.all(this.items.map(item => item.filterRecords(offsetInfo, pageNo, records, filteredRecordIndices, cache)))
    return records;
  }
}

class OrReader extends MultiReader {
  matchesRowGroupStats(rowGroup) {
    return this.items.some(item => item.matchesRowGroupStats(rowGroup));
  }

  async findIgnorePages(rowGroup, ignoredPageSet, cache) {
    if (ignoredPageSet === undefined) {
      ignoredPageSet = new Set();
    }

    // any item can remove something from the ignored page set
    let setsToIntersect = await Promise.all(this.items.map(item => item.findIgnorePages(rowGroup, new Set(), cache)));

    let firstSet = setsToIntersect[0];
    let otherSets = setsToIntersect.slice(1);
    [...firstSet].forEach(pageIndex => {
      if (otherSets.length === 0 || otherSets.every(otherSet => otherSet.has(pageIndex))) {
        ignoredPageSet.add(pageIndex);
      }
    });

    return ignoredPageSet;
  }

  async filterRecords(offsetInfo, pageNo, records, filteredRecordIndices, cache) {
    let setsToIntersect = await Promise.all(this.items.map(async item => {
      let s = new Set(filteredRecordIndices);
      await item.filterRecords(offsetInfo, pageNo, records, s, cache);
      return s;
    }));

    let firstSet = setsToIntersect[0];
    let otherSets = setsToIntersect.slice(1);
    [...firstSet].forEach(recordIndex => {
      if (otherSets.length === 0 || otherSets.every(otherSet => otherSet.has(recordIndex))) {
        filteredRecordIndices.add(recordIndex);
      }
    });

    return records;
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

  async findIgnorePages(rowGroup, ignoredPageSet, cache) {
    return ignoredPageSet;
  }

  async loadOffsetInfo(rowGroup, target, cache) {
    if (!cache[this.columnIndex]) {
      cache[this.columnIndex] = true;
      const column = rowGroup.columns[this.columnIndex];
      let offsetIndex = await this.reader.readOffsetIndex(column);
      target[this.columnIndex] = offsetIndex;
      target.pageCount = offsetIndex.page_locations.length;
    }
    return target;
  }

  async filterRecords(offsetInfo, pageNo, records, cache) {
    return records;
  }

  async loadRecords(offsetInfo, pageNo, records, cache) {
    if (this.index) {
      return records;
    }

    return await (cache[this.columnIndex] || (cache[this.columnIndex]=this.reader.readPage(offsetInfo[this.columnIndex], pageNo, records)));
  }
}

class RangeReaderItem extends PathReaderItem {
  constructor(item) {
    super(item);
    this.min = item.min;
    this.max = item.max;
  }

  matchesRowGroupStats(rowGroup) {
    const column = rowGroup.columns[this.columnIndex];
    const stats = column.meta_data.statistics;

    return ((this.min === undefined || stats.max_value >= this.min) && 
            (this.max === undefined || stats.min_value <= this.max));
  }

  async findIgnorePages(rowGroup, ignoredPageSet, cache) {
    let colIndex = await (cache[this.columnIndex] || (cache[this.columnIndex]=this.reader.readColumnIndex(rowGroup.columns[this.columnIndex])));
    
    colIndex.max_values.forEach( (max, pageNo) => {
      let min = colIndex.min_values[pageNo];
      if (
        (this.min !== undefined && this.min > max) ||
        (this.max !== undefined && this.max < min)
      ) {
        ignoredPageSet.add(pageNo);
      }
    });

    return ignoredPageSet;
  }

  async filterRecords(offsetInfo, pageNo, records, filteredRecordIndices, cache) {
    if (this.index) {
      return records;
    }

    await this.loadRecords(offsetInfo, pageNo, records, cache);
    records.forEach((record, index) => {
      let value = record[this.path];
      if ((this.min !== undefined && value < this.min) ||
          (this.max !== undefined && value > this.max)) {
        filteredRecordIndices.add(index);
      }
    });
    return records;
  }
}

class ValueReaderItem extends PathReaderItem {
  constructor(item) {
    super(item);
    this.value = item.value;
  }

  matchesRowGroupStats(rowGroup) {
    const column = rowGroup.columns[this.columnIndex];
    const stats = column.meta_data.statistics;

    return (stats.min_value <= this.value && stats.max_value >= this.value);
  }

  async findIgnorePages(rowGroup, ignoredPageSet, cache) {
    let colIndex = await (cache[this.columnIndex] || (cache[this.columnIndex]=this.reader.readColumnIndex(rowGroup.columns[this.columnIndex])));
    
    colIndex.max_values.forEach( (max, pageNo) => {
      let min = colIndex.min_values[pageNo];
      if (this.min > this.value || this.max < this.value) {
        ignoredPageSet.add(pageNo);
      }
    });

    return ignoredPageSet;
  }

  async filterRecords(offsetInfo, pageNo, records, filteredRecordIndices, cache) {
    if (this.index) {
      return records;
    }
    
    await this.loadRecords(offsetInfo, pageNo, records, cache);
    records.forEach((record, index) => {
      let value = record[this.path];
      if (this.value != value) {
        filteredRecordIndices.add(index);
      }
    });
    return records;
  }
}

class PathReader extends AndReader  {
  constructor(reader, paths) {
    super(paths);
    this.prepareColumns(reader);
  }
}

module.exports = PathReader;