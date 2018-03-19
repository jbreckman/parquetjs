const QuickLRU = require('quick-lru');
const lru = new QuickLRU({maxSize: (+process.env.PARQUET_CACHE_SIZE) || 10000});
const shortMap = new Map();

function memoize(type, reader, rowGroup, path, page, skipCache, timer, fn) {
  let key = `${reader.id}_rowgroup${rowGroup.no}_${type}_${path}_${page || ''}`;
  let map = skipCache ? shortMap : lru;
  timer && timer('read', type, key);

  let result = map.get(key);
  if (!result) {
    timer && timer('miss', type, key);
    let startTime = new Date().getTime();
    result = fn().then(d => {
      timer && timer('complete', type, key, `${new Date().getTime() - startTime}ms`);
      if (skipCache) {
        map.delete(key);
      }
      return d;
    });
    map.set(key, result);
  }
  return result;
}

class RowRange {
  constructor(reader, rowGroup, timer) {
    this.reader = reader;
    this.rowGroup = rowGroup;
    this.timer = timer;

    this._minValues = {};
    this._maxValues = {};
    this._offsetIndexPromises = {};
    this._offsetIndex = {};
    this._columnIndexPromises = {};
    this._columnIndex = {};

    this.columnLookup = rowGroup.columns.reduce((acc, column) => {
      acc[column.meta_data.path_in_schema.join(',')] = column;
      return acc;
    }, {});

    this.lowIndex = 0;
    this.highIndex = +rowGroup.num_rows - 1;
  }

  extend(lowIndex, highIndex, path, pathLowValue, pathHighValue) {
    let result = Object.setPrototypeOf({}, this);
    result._minValues = Object.setPrototypeOf({}, result._minValues);
    result._maxValues = Object.setPrototypeOf({}, result._maxValues);
    
    result.lowIndex = lowIndex;
    result.highIndex = highIndex;
    if (path) {
      result._minValues[path] = pathLowValue;
      result._maxValues[path] = pathHighValue;
    }

    return result;
  }

  minValue(path) {
    let minValue = this._minValues[path];
    if (minValue === undefined) {
      return this.columnLookup[path].meta_data.statistics.min_value;
    }
    return minValue;
  }

  maxValue(path) {
    let maxValue = this._maxValues[path];
    if (maxValue === undefined) {
      return this.columnLookup[path].meta_data.statistics.max_value;
    }
    return maxValue;
  }

  primeOffsetIndex(path) {
    return memoize('offsetIndex', this.reader, this.rowGroup, path, null, false, this.timer, 
      () => this.reader.readOffsetIndex(this.columnLookup[path]))
      .then(d => {
        this._offsetIndex[path] = d;
        return d;
      });
  }

  primeColumnIndex(path) {
    return memoize('columnIndex', this.reader, this.rowGroup, path, null, false, this.timer, 
      () => this.reader.readColumnIndex(this.columnLookup[path]))
      .then(d => {
        this._columnIndex[path] = d;
        return d;
      });
  }

  pageData(path, pageNo, cache) {
    return memoize('page', this.reader, this.rowGroup, path, pageNo, !cache, this.timer, 
      () => this.reader.readFlatPage(this._offsetIndex[path], pageNo));
  }

  prime(path, offsetIndex, columnIndex) {
    let promises = [];
    if (offsetIndex) {
      promises.push(this.primeOffsetIndex(path));
    }
    if (columnIndex) {
      promises.push(this.primeColumnIndex(path));
    }
    return Promise.all(promises).then(d => this);
  }

  findRelevantPageIndex(path, rowIndex) {
    let pageLocations = this._offsetIndex[path].page_locations;
    // do a simple binary search to find the relevant page location
    let lowIndex = 0, highIndex = pageLocations.length-1;
    while (lowIndex < highIndex) {
      let midPoint = Math.ceil((highIndex - lowIndex) / 2) + lowIndex;
      let midPointValue = pageLocations[midPoint].first_row_index;
      if (midPointValue <= rowIndex) {
        lowIndex = midPoint;
      }
      else if (highIndex !== midPoint) {
        highIndex = midPoint;
      }
      else {
        highIndex = midPoint-1;
      }
    }
    return lowIndex;
  }
}

module.exports = RowRange;