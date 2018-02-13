'use strict';
const QuickLRU = require('quick-lru');
const lru = new QuickLRU({maxSize: (+process.env.PARQUET_CACHE_SIZE) || 10000});

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

  loadOffsetInfo(rowGroup, target) {
    if (!target) {
      target = {};
    }
    return Promise.all(this.items.map(item => item.loadOffsetInfo(rowGroup, target))).then(() => target);
  }

  getRecordCount(rowGroup) {
    let columnIndex = this.firstColumnIndex(),
        reader = this.reader;

    return memoize(`${reader.id}_columnIndex${columnIndex}`, () => reader.readColumnIndex(rowGroup.columns[0]))
      .then(someColInfo => someColInfo.max_values.length);
  }

  findRelevantPages(rowGroup) {
    return Promise.all([this.preloadRelevantPageInfo(rowGroup), this.getRecordCount(rowGroup)])
      .then(r => { // move to spread if we use bluebird
        let cb = r[0];
        let rowCount = r[1];

        let results = [];
        for (var i = 0; i < rowCount; i++) {
          if (cb(i)) {
            results.push(i);
          }
        }
        return results;
      });
  }

  getRecords(offsetInfo, pageNo, rowGroupNo) {
    let records = [];

    records._columnsLoaded = new Set();
    return this.loadFilteredFields(offsetInfo, pageNo, records, rowGroupNo).then(records => {
      let filteredResults = null;

      // we might not have any filtered fields to load, and if that's the case then
      // don't worry about it.  but if there are filtered records, then we shoul see
      // if we should short circuit
      if (records.length > 0) {
        filteredResults = [];
        for (var i = 0; i < records.length; i++) {
          let record = records[i];

          if (this.filterRecord(record)) {
            filteredResults.push(record);
          }
          else {
            record._remove = true;
          }
        }
        if (filteredResults.length === 0) {
          return [];
        }
      }

      return this.loadRecords(offsetInfo, pageNo, records, rowGroupNo).then(() => filteredResults || records);
    });
  }

  loadRecords(offsetInfo, pageNo, records, rowGroupNo) {
    return Promise.all(this.items.map(item => item.loadRecords(offsetInfo, pageNo, records, rowGroupNo)))
      .then(() => records);
  }

  loadFilteredFields(offsetInfo, pageNo, records, rowGroupNo) {
    return Promise.all(this.items.map(item => item.loadFilteredFields(offsetInfo, pageNo, records, rowGroupNo)))
      .then(() => records);
  }
}

class AndReader extends MultiReader {
  matchesRowGroupStats(rowGroup) {
    for (var i = 0; i < this.items.length; i++) {
      if (!this.items[i].matchesRowGroupStats(rowGroup)) {
        return false;
      }
    }
    return true;
  }

  filterRecord(record) {
    for (var i = 0; i < this.items.length; i++) {
      if (!this.items[i].filterRecord(record)) {
        return false;
      }
    }
    return true;
  }

  preloadRelevantPageInfo(rowGroup) {
    return Promise.all(this.items.map(item => item.preloadRelevantPageInfo(rowGroup))).then(items => pageIndex => {
      for (var i = 0; i < items.length; i++) {
        if (!items[i](pageIndex)) {
          return false;
        }
      }
      return true;
    });
  }
}

class OrReader extends MultiReader {
  matchesRowGroupStats(rowGroup) {
    for (var i = 0; i < this.items.length; i++) {
      if (this.items[i].matchesRowGroupStats(rowGroup)) {
        return true;
      }
    }
    return false;
  }

  filterRecord(record) {
    for (var i = 0; i < this.items.length; i++) {
      if (this.items[i].filterRecord(record)) {
        return true;
      }
    }
    return false;
  }

  preloadRelevantPageInfo(rowGroup) {
    return Promise.all(this.items.map(item => item.preloadRelevantPageInfo(rowGroup))).then(items => pageIndex => {
      for (var i = 0; i < items.length; i++) {
        if (items[i](pageIndex)) {
          return true;
        }
      }
      return false;
    });
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

  loadOffsetInfo(rowGroup, target) {  
    let columnIndex = this.columnIndex;
    const column = rowGroup.columns[columnIndex];
    return memoize(`${this.reader.id}_offsetIndex${columnIndex}`, () => this.reader.readOffsetIndex(column))
      .then(offsetIndex => {
        target[columnIndex] = offsetIndex;
        target.pageCount = offsetIndex.page_locations.length;
      });
  }

  preloadRelevantPageInfo(rowGroup) {
    return () => true;
  }

  _loadRecords(offsetInfo, pageNo, records, rowGroupNo) {
    let columnIndex = this.columnIndex,
        reader = this.reader,
        source = this.source,
        path = this.path;

    if (records._columnsLoaded.has(columnIndex)) {
      return;
    }
    records._columnsLoaded.add(columnIndex);

    return memoize(`${reader.id}_page_${rowGroupNo}_${pageNo}_${columnIndex}_${source}`, () => reader.readPage(offsetInfo[columnIndex], pageNo, []).then(r => {
        if (source) {
          let result = [];
          for (var i = 0; i < r.length; i++) {
            result.push(Object(r[i][path]));
          }
          return result;
        }
        else {
          let result = [];
          for (var i = 0; i < r.length; i++) {
            result.push(r[i][path]);
          }
          return result;
        }
      }))
      .then(columnRecords => {

        if (source) {
          if (records.length === 0) {
            for (var i = 0; i < columnRecords.length; i++) {
              let newRecord = {};

              let columnRecord = columnRecords[i];
              if (!columnRecord.json) {
                columnRecord.json = JSON.parse(columnRecord.toString());
              }
              let json = columnRecord.json;

              for (var key in json) {
                newRecord[key] = json[key];
              }
              records.push(newRecord);
            }
          }
          else {
            for (var i = 0; i < columnRecords.length; i++) {
              let record = records[i];
              if (!record._remove) {
                let columnRecord = columnRecords[i];
                if (!columnRecord.json) {
                  columnRecord.json = JSON.parse(columnRecord.toString());
                }
                let json = columnRecord.json;
                for (var key in json) {
                  record[key] = json[key];
                }
              }
            }
          }
        }
        else {
          if (records.length === 0) {
            for (var i = 0; i < columnRecords.length; i++) {
              records.push({[path]: columnRecords[i]});
            }
          }
          else {
            for (var i = 0; i < columnRecords.length; i++) {
              let record = records[i];
              if (!record._remove) {
                record[path] = columnRecords[i];
              }
            }
          }
        }
        return records;
      });
  }

  loadFilteredFields(offsetInfo, pageNo, records, rowGroupNo) {
    if (!this.filtered || this.index) {
      return records;
    }
    return this._loadRecords(offsetInfo, pageNo, records, rowGroupNo);
  }

  loadRecords(offsetInfo, pageNo, records, rowGroupNo) {
    if (this.index) {
      return records;
    }
    return this._loadRecords(offsetInfo, pageNo, records, rowGroupNo);
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

  preloadRelevantPageInfo(rowGroup) {
    let min = this.min,
        max = this.max,
        reader = this.reader,
        columnIndex = this.columnIndex;

    return memoize(`${reader.id}_columnIndex${columnIndex}`, () => reader.readColumnIndex(rowGroup.columns[columnIndex]))
      .then(colInfo => pageIndex => {
        if ((min !== undefined && min > colInfo.max_values[pageIndex]) ||
            (max !== undefined && max < colInfo.min_values[pageIndex])) {
          return false;
        }
        return true;
      });
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

  preloadRelevantPageInfo(rowGroup) {
    let value = this.value,
        columnIndex = this.columnIndex,
        reader = this.reader;

    return memoize(`${reader.id}_columnIndex${columnIndex}`, () => reader.readColumnIndex(rowGroup.columns[columnIndex]))
      .then(colInfo => pageIndex => (colInfo.min_values[pageIndex] <= value && colInfo.max_values[pageIndex] >= value));
  }
}

class PathReader extends AndReader  {
  constructor(reader, paths) {
    super(paths);
    this.prepareColumns(reader);
  }
}

module.exports = PathReader;