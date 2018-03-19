
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

  _id(path, page) {
    return `${this.reader.id}_rowgroup${this.rowGroup.no}_${path}_${page || ''}`
  }
  _time() {
    var start = new Date().getTime();
    return function() {
      return `${new Date().getTime() - start}ms`;
    }
  }

  primeOffsetIndex(path) {
    this.timer && this.timer('read', 'offsetIndex', this._id(path));
    if (this._offsetIndexPromises[path]) {
      return this._offsetIndexPromises[path];
    }

    this.timer && this.timer('miss', 'offsetIndex', this._id(path));
    let t = this.timer && this._time();
    return this._offsetIndexPromises[path] = this.reader.readOffsetIndex(this.columnLookup[path])
      .then(d => {
        this.timer && this.timer('complete', 'offsetIndex', this._id(path), t());
        this._offsetIndex[path] = d;
        return d;
      });
  }

  primeColumnIndex(path) {
    this.timer && this.timer('read', 'columnIndex', this._id(path));
    if (this._columnIndexPromises[path]) {
      return this._columnIndexPromises[path];
    }

    this.timer && this.timer('miss', 'columnIndex', this._id(path));
    let t = this.timer && this._time();
    return this._columnIndexPromises[path] = this.reader.readColumnIndex(this.columnLookup[path])
      .then(d => {
        this.timer && this.timer('complete', 'columnIndex', this._id(path), t());
        this._columnIndex[path] = d;
        return d;
      });
  }

  pageData(path, pageNo) {
    this.timer && this.timer('read', 'page', this._id(path, pageNo));
    return this.reader.readFlatPage(this._offsetIndex[path], pageNo);
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