const etl = require('etl');
const streamz = require('streamz');

const CONCURRENCY = 500;

/*

Sample "spec":

{
  filter: [
    // phase 1 of filters
    [
      { path: 'name', value: 'Josh', bloom: true },
      { path: 'gender', value: 'male' }
    ],
    // phase 2 of filters
    [
      { 
        or: [
          { path: 'age', min: 30, max: 40 },
          { path: 'age', min: 80, max: 90 }
        ]
      }
    ]
  ],
  fields: [
    { path: 'name' },
    { path: 'source', source: true },
  ],
  post: [
    { type: 'filter', script: fn },
    { type: 'transform', script: fn },
    { type: 'filter', script: fn }
  ]
}

*/

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
    return this.reader.readFlatPage(this._offsetIndex[path], pageNo, []);
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

function parseFilterPhase(spec) {
  if (spec.index) {
    if (spec.value !== undefined) {
      return new FilterValueIndexPhase(spec);
    }
    else if (spec.min !== undefined || spec.max !== undefined) {
      return new FilterRangeIndexPhase(spec);
    }
  }
  else if (spec.value !== undefined) {
    return new FilterValuePhase(spec);
  }
  else if (spec.min !== undefined || spec.max !== undefined) {
    return new FilterRangePhase(spec);
  }
  else if (spec.or) {
    return new FilterOrPhase(spec.or);
  }
  else if (Array.isArray(spec)) {
    return new FilterAndPhase(spec);
  }
  else if (spec.and) {
    return new FilterAndPhase(spec.and);
  }
}

class FilterMultiItemPhase {
  constructor(items) {
    this.items = items.map(item => parseFilterPhase(item));
  }

  prime(rowRange) {
    let promises = [];
    for (var i = 0; i < this.items.length; i++) {
      promises.push(this.items[i].prime(rowRange));
    }
    return Promise.all(promises).then(d => rowRange);
  }
}

class FilterAndPhase extends FilterMultiItemPhase {
  pipe() {
    let items = this.items;
    return etl.chain(stream => {

      // first warn all our items that this range is coming
      // let them get a jump on some metadata if they want it
      let result = stream.pipe(etl.map(rowRange => {
        if (this.fastFilter(rowRange)) {
          return this.prime(rowRange);
        }
      }, { concurrency: CONCURRENCY }));

      for (var i = 0; i < items.length; i++) {
        let item = items[i];
        result = result.pipe(item.pipe());
      }

      return result;
    });
  }

  fastFilter(rowRange) {
    for (var i = 0; i < this.items.length; i++) {
      if (!this.items[i].fastFilter(rowRange)) {
        return false;
      }
    }
    return true;
  }
}

class FilterOrPhase extends FilterMultiItemPhase {
  
  pipe() {
    let items = this.items;
    return etl.map(function(rowRange) {
      let targetStream = this,
          sentIndices = [],
          startingIndex = rowRange.lowIndex;

      return Promise.all(items.map(item => etl.toStream([rowRange])
        .pipe(item.pipe())
        .pipe(etl.map(rowRange => {

          let lastUnsentStartIndex = undefined;
          for (var i = rowRange.lowIndex; i <= rowRange.highIndex; i++) {
            if (sentIndices[i - startingIndex]) {
              if (lastUnsentStartIndex !== undefined) {
                targetStream.push(rowRange.extend(lastUnsentStartIndex, i - 1));
                lastUnsentStartIndex = undefined;
              }
            }
            else {
              sentIndices[i - startingIndex] = true;
              if (lastUnsentStartIndex === undefined) {
                lastUnsentStartIndex = i;
              }
            }
          }

          if (lastUnsentStartIndex !== undefined) {
            targetStream.push(rowRange.extend(lastUnsentStartIndex, rowRange.highIndex));
          }

        }, { concurrency: CONCURRENCY }))
        .promise()))
      .then(d => {});
    }, { concurrency: CONCURRENCY });
  }


  fastFilter(rowRange) {
    for (var i = 0; i < this.items.length; i++) {
      if (this.items[i].fastFilter(rowRange)) {
        return true;
      }
    }
    return false;
  }
}


//
// Handle fast filters of records by looking at indices
//
class FilterIndexPhase {

  constructor(path) {
    this.path = path;
  }

  fastFilter(rowRange) {
    // can we cancel this out immediately?  if so let's do that
    let rowRangeMinValue = rowRange.minValue(this.path),
        rowRangeMaxValue = rowRange.maxValue(this.path);
    if (rowRangeMinValue !== undefined && rowRangeMaxValue !== undefined &&
        !this.evaluate(rowRangeMinValue, rowRangeMaxValue)) {
      return false;
    }
    return true;
  }

  prime(rowRange) {
    return rowRange.prime(this.path, true, true);
  }

  pipe() {
    let path = this.path,
        evaluate = this.evaluate.bind(this),
        fastFilter = this.fastFilter.bind(this);

    return etl.map(function(rowRange) {

      if (!fastFilter(rowRange)) {
        return;
      }

      let columnIndex = rowRange._columnIndex[path];
      let columnOffsets = rowRange._offsetIndex[path].page_locations;
      let startingPage = rowRange.findRelevantPageIndex(path, rowRange.lowIndex);
      let endingPage = rowRange.findRelevantPageIndex(path, rowRange.highIndex);

      let nextRangeStartingIndex = undefined,
          nextRangeEndIndex = undefined,
          nextRangeLowValue = undefined,
          nextRangeHighValue = undefined;

      for (var i = startingPage; i <= endingPage; i++) {
        let maxValue = columnIndex.max_values[i];
        let minValue = columnIndex.min_values[i];

        if (evaluate(minValue, maxValue)) {
          // extend or build new range
          nextRangeEndIndex = Math.min(rowRange.highIndex, i < columnOffsets.length - 1 ? columnOffsets[i + 1].first_row_index - 1 : Infinity);
          if (nextRangeStartingIndex === undefined) {
            nextRangeStartingIndex = Math.max(rowRange.lowIndex, columnOffsets[i].first_row_index);
            nextRangeLowValue = minValue;
            nextRangeHighValue = maxValue;
          }
          else {
            nextRangeLowValue = nextRangeLowValue < minValue ? nextRangeLowValue : minValue;
            nextRangeHighValue = nextRangeHighValue > maxValue ? nextRangeHighValue : maxValue;
          }
        }
        else if (nextRangeStartingIndex !== undefined) {
          // flush the next range (if we have it)
          this.push(rowRange.extend(nextRangeStartingIndex, nextRangeEndIndex, path, nextRangeLowValue, nextRangeHighValue));
          nextRangeStartingIndex = undefined;
          nextRangeLowValue = undefined;
          nextRangeHighValue = undefined;
          nextRangeEndIndex = undefined;
        }
      }

      // leftover end range?
      if (nextRangeStartingIndex !== undefined) {
        this.push(rowRange.extend(nextRangeStartingIndex, nextRangeEndIndex, path, nextRangeLowValue, nextRangeHighValue));
      }

    }, { concurrency: CONCURRENCY });
  }
}

class FilterRangeIndexPhase extends FilterIndexPhase {
  constructor(spec) {
    super(spec.path);
    this.min = spec.min;
    this.max = spec.max;
    this.sMin = spec.min === undefined ? undefined : String(spec.min);
    this.sMax = spec.max === undefined ? undefined : String(spec.max);
  }

  evaluate(minValue, maxValue) {
    if (this.max !== undefined && this.max < minValue) {
      return false;
    }
    if (this.min !== undefined && this.min > maxValue) {
      return false;
    }
    return true;
  }
}

class FilterValueIndexPhase extends FilterIndexPhase {
  constructor(spec) {
    super(spec.path);
    this.value = spec.value;
    this.sValue = String(spec.value);
  }

  evaluate(minValue, maxValue) {
    if (minValue > this.value) {
      return false;
    }
    if (maxValue < this.value) {
      return false;
    }
    return true;
  }
}




//
// Handle slow filters of records 
//
class FilterPhase {

  constructor(path) {
    this.path = path;
  }

  prime(rowRange) {
    return rowRange.prime(this.path, true, false);
  }

  fastFilter(rowRange) {
    return true;
  }

  fastPass(rowRange) {
    return true;
  }

  pipe() {
    let path = this.path,
        evaluate = this.evaluate.bind(this),
        fastFilter = this.fastFilter.bind(this),
        fastPass = this.fastPass.bind(this);

    return etl.chain(stream => {

      // split this into messages in pipe per page 
      // to be handle pressure
      return stream.pipe(etl.map(function(rowRange) {
        if (!fastFilter(rowRange)) {
          return;
        }
        if (fastPass(rowRange)) {
          this.push(rowRange);
          return;
        }

        let columnIndex = rowRange._columnIndex[path];
        let columnOffsets = rowRange._offsetIndex[path].page_locations;
        let startingPage = rowRange.findRelevantPageIndex(path, rowRange.lowIndex);
        let endingPage = rowRange.findRelevantPageIndex(path, rowRange.highIndex);

        for (var i = startingPage; i <= endingPage; i++) {
          let startIndex = i === startingPage ? rowRange.lowIndex : columnOffsets[i].first_row_index;
          let endingIndex = i === endingPage ? rowRange.highIndex : columnOffsets[i + 1].first_row_index - 1;

          if (columnIndex) {
            this.push(rowRange.extend(startIndex, endingIndex, path, columnIndex.min_values[i], columnIndex.max_values[i]));
          }
          else {
            this.push(rowRange.extend(startIndex, endingIndex));
          }
        }
      }, { concurrency: CONCURRENCY }))
      .pipe(etl.map(function(rowRange) {
        if (fastPass(rowRange)) {
          this.push(rowRange);
          return;
        }

        // we know that row range is now in one and only page  
        let columnOffsets = rowRange._offsetIndex[path].page_locations;
        let pageIndex = rowRange.findRelevantPageIndex(path, rowRange.lowIndex);
        let startingPageRowIndex = columnOffsets[pageIndex].first_row_index;
        return rowRange.pageData(path, pageIndex).then(values => {

          let nextRangeStartingIndex = undefined,
              nextRangeEndIndex = undefined,
              nextRangeLowValue = undefined,
              nextRangeHighValue = undefined;

          for (var i = rowRange.lowIndex; i <= rowRange.highIndex; i++) {
            let relativeIndex = i - startingPageRowIndex;
            let value = values[relativeIndex];
            if (evaluate(value)) {
              // extend or build new range
              nextRangeEndIndex = i;
              if (nextRangeStartingIndex === undefined) {
                nextRangeStartingIndex = i;
                nextRangeLowValue = value;
                nextRangeHighValue = value;
              }
              else {
                nextRangeLowValue = nextRangeLowValue < value ? nextRangeLowValue : value;
                nextRangeHighValue = nextRangeHighValue > value ? nextRangeHighValue : value;
              }
            }
            else if (nextRangeStartingIndex !== undefined) {
              // flush the next range (if we have it)
              this.push(rowRange.extend(nextRangeStartingIndex, nextRangeEndIndex, path, nextRangeLowValue, nextRangeHighValue));
              nextRangeStartingIndex = undefined;
              nextRangeLowValue = undefined;
              nextRangeHighValue = undefined;
              nextRangeEndIndex = undefined;
            }
          }

          // leftover end range?
          if (nextRangeStartingIndex !== undefined) {
            this.push(rowRange.extend(nextRangeStartingIndex, nextRangeEndIndex, path, nextRangeLowValue, nextRangeHighValue));
          }
        })
      }, { concurrency: CONCURRENCY }));
    });
  }
}

class FilterRangePhase extends FilterPhase {
  constructor(spec) {
    super(spec.path);
    this.min = spec.min;
    this.max = spec.max;
    this.sMin = spec.min === undefined ? undefined : String(spec.min);
    this.sMax = spec.max === undefined ? undefined : String(spec.max);
  }

  fastFilter(rowRange) {
    // can we cancel this out immediately?  if so let's do that
    let rowRangeMinValue = rowRange.minValue(this.path),
        rowRangeMaxValue = rowRange.maxValue(this.path);
    if (rowRangeMinValue !== undefined && rowRangeMaxValue !== undefined &&
        (this.max !== undefined && rowRangeMinValue > this.max) || (this.min !== undefined && rowRangeMaxValue < this.min)) {
      return false;
    }
    return true;
  }

  fastPass(rowRange) {
    // can we pass the whole range immediatley?  if so, let's not read the data
    let rowRangeMinValue = rowRange.minValue(this.path),
        rowRangeMaxValue = rowRange.maxValue(this.path);
    if (rowRangeMinValue !== undefined && rowRangeMaxValue !== undefined &&
        (this.min === undefined || rowRangeMinValue > this.min) && (this.max === undefined || rowRangeMaxValue < this.max)) {
      return true;
    }
    return false;
  }

  evaluate(value) {
    if (this.max !== undefined && this.max < value) {
      return false;
    }
    if (this.min !== undefined && this.min > value) {
      return false;
    }
    return true;
  }

}

class FilterValuePhase extends FilterPhase {
  constructor(spec) {
    super(spec.path);
    this.value = spec.value;
    this.sValue = String(spec.value);
  }

  evaluate(value) {
    return value == this.value;
  }

  fastFilter(rowRange) {
    // can we cancel this out immediately?  if so let's do that
    let rowRangeMinValue = rowRange.minValue(this.path),
        rowRangeMaxValue = rowRange.maxValue(this.path);
    if (rowRangeMinValue !== undefined && rowRangeMaxValue !== undefined &&
        (rowRangeMinValue > this.value || rowRangeMaxValue < this.value)) {
      return false;
    }
    return true;
  }

  fastPass(rowRange) {
    // can we pass the whole range immediatley?  if so, let's not read the data
    let rowRangeMinValue = rowRange.minValue(this.path),
        rowRangeMaxValue = rowRange.maxValue(this.path);
    if (rowRangeMinValue !== undefined && rowRangeMaxValue === rowRangeMinValue &&
        rowRangeMinValue == this.value) {
      return true;
    }
    return false;
  }
}

class PathStreamer {
  constructor(spec, readers, timer) {
    this.timer = timer;

    this.rootStream = streamz(); 
    readers.forEach(reader => reader.metadata.row_groups.forEach((rowGroup, index) => {
      rowGroup.no = index;
      this.rootStream.write(new RowRange(reader, rowGroup, timer));
    }));
    this.rootStream.end();

    this.stream = this.rootStream;
    if (spec.filter) {
      spec.filter.forEach(filterSpec => {
        this.stream = this.stream.pipe(parseFilterPhase(Array.isArray(filterSpec) ? filterSpec : [filterSpec]).pipe());
      });
    }

    if (spec.fields) {
      this.stream = this.stream.pipe(this.loadFields(spec.fields));
    }

    if (spec.post) {
      spec.post.forEach(post => {
        if (post.type === 'filter') {
          let script = post.script;
          this.stream = this.stream.pipe(etl.map(function(d) {
            if (script(d)) {
              this.push(d);
            }
          }));
        }
        else if (post.type === 'transform') {
          let script = post.script;
          this.stream = this.stream.pipe(etl.map(function(d) {
            this.push(script(d));
          }));
        }
      });
    }
  }

  loadFields(fields) {
    return etl.chain(stream => {
      return stream.pipe(etl.map(function(rowRange) {

        // first load all the offset indices
        return Promise.all(fields.map(field => rowRange.primeOffsetIndex(field.path)))
          .then(offsetIndices => {

            // now we want to break it down so each row range is only on one page per path
            let fieldPageIndices = fields.map(field => rowRange.findRelevantPageIndex(field.path, rowRange.lowIndex));
            let lowIndex = rowRange.lowIndex;

            while (true) {
              let lowestNextPageIndex = Infinity, lowestNextPageFieldIndex = -1;
              for (var i = 0; i < fieldPageIndices.length; i++) {
                let nextPageLocation = offsetIndices[i].page_locations[fieldPageIndices[i] + 1];
                if (nextPageLocation && nextPageLocation.first_row_index < lowestNextPageIndex) {
                  lowestNextPageIndex = nextPageLocation.first_row_index;
                  lowestNextPageFieldIndex = i;
                }
              }

              if (lowestNextPageIndex <= rowRange.highIndex) {
                this.push(rowRange.extend(lowIndex, lowestNextPageIndex - 1));
                fieldPageIndices[lowestNextPageFieldIndex]++;
                lowIndex = lowestNextPageIndex;
              }
              else {
                this.push(rowRange.extend(lowIndex, rowRange.highIndex));
                return;
              }
            }
          });
      }, { concurrency: CONCURRENCY }))
      .pipe(etl.map(function(rowRange) {
        let columnOffsets = fields.map(field => rowRange._offsetIndex[field.path].page_locations);
        let fieldPageIndices = fields.map(field => rowRange.findRelevantPageIndex(field.path, rowRange.lowIndex));
        return Promise.all(fieldPageIndices.map((pageIndex, fieldIndex) => rowRange.pageData(fields[fieldIndex].path, pageIndex)))
          .then(pageData => {
            for (var i = rowRange.lowIndex; i <= rowRange.highIndex; i++) {
              let value = {};
              for (var j = 0; j < fields.length; j++) {
                let path = fields[j].path;
                let pageIndex = fieldPageIndices[j];
                let pageStartIndex = columnOffsets[j][pageIndex].first_row_index;
                value[path] = pageData[j][i - pageStartIndex];
              }
              this.push(value);
            }
          });
      }, { concurrency: CONCURRENCY }));
    });
  }
}

module.exports = PathStreamer;