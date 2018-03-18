'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet = require('../parquet.js');
const PathStreamer = require('../lib/pathStreamer');

describe('PathStreamer', function() {

  function mockColumn(path, minValue, maxValue, startingIndices, minValues, maxValues) {
    return {
      offsetIndex: {
        page_locations: startingIndices.map(startingIndex => ({
          first_row_index: startingIndex
        }))
      },
      columnIndex: {
        min_values: minValues,
        max_values: maxValues
      },
      meta_data: {
        path_in_schema: [path],
        statistics: {
          min_value: minValue,
          max_value: maxValue
        }
      }
    }
  }

  function mockReader() {
    let result = {
      metadata: {
        row_groups: [
          {
            columns: [mockColumn('quantity', 20, 30, [0, 4], [20, 25], [30, 29]), mockColumn('name', "abbot", "miles", [0], ["abbot"], ["miles"])],
            num_rows: 6,
            pageData: { // mock purposes only
              quantity: [[20, 25, 29, 30], [29, 25]],
              name: [['abbot', 'dallas', 'bilbo','charles', 'josh', 'miles']]
            }
          },
          {
            columns: [mockColumn('quantity', 15, 32, [0, 1, 3], [20, 15, 18], [20, 17, 30]), mockColumn('name', "nick", "zane", [0], ["nick"], ["zane"])],
            num_rows: 5,
            pageData: { // mock purposes only
              quantity: [[20],[17,15],[30,18]],
              name: []
            }
          }
        ]
      },
      readOffsetIndex: column => Promise.resolve(column.offsetIndex),
      readColumnIndex: column => Promise.resolve(column.columnIndex),
      readPage: (column, pageIndex) => Promise.resolve(column._pageData[pageIndex])
    };
    result.metadata.row_groups.forEach(rowGroup => {
      rowGroup.columns.forEach(column => {
        column._pageData = rowGroup.pageData[column.meta_data.path_in_schema.join('.')];
      });
    });
    return result;
  }

  it('reader statistics winnowing - range', async function() {
    let spec = {
      filter: [
        { path: 'quantity', min: 5, max: 10, index: true }
      ]
    }
    
    let reader = mockReader();
    reader.readOffsetIndex = () => Promise.reject('SHOULDNT GET HIT');
    reader.readColumnIndex = () => Promise.reject('SHOULDNT GET HIT');
    let pathStreamer = new PathStreamer(spec, [reader]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 0);
  });
  

  it('reader statistics winnowing - low range', async function() {
    let spec = {
      filter: [
        { path: 'quantity', min: 5, max: 18, index: true }
      ]
    }
    
    let pathStreamer = new PathStreamer(spec, [mockReader()]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 1);
    assert.equal(results[0].rowGroup.num_rows, 5);
    assert.equal(results[0].lowIndex, 1);
    assert.equal(results[0].highIndex, 4);
  });


  it('reader statistics winnowing - three hits', async function() {
    let spec = {
      filter: [
        { path: 'quantity', min: 18, max: 20, index: true }
      ]
    }
    
    let pathStreamer = new PathStreamer(spec, [mockReader()]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 3);
    assert.equal(results[0].rowGroup.num_rows, 6);
    assert.equal(results[0].lowIndex, 0);
    assert.equal(results[0].highIndex, 3);
    assert.equal(results[1].rowGroup.num_rows, 5);
    assert.equal(results[1].lowIndex, 0);
    assert.equal(results[1].highIndex, 0);
    assert.equal(results[2].rowGroup.num_rows, 5);
    assert.equal(results[2].lowIndex, 3);
    assert.equal(results[2].highIndex, 4);
  });




  it('reader statistics winnowing - value', async function() {
    let spec = {
      filter: [
        [ { path: 'quantity', value: 5, index: true },
          { path: 'name', value: 'josh', index: true } ] // would normally hit... but won't
      ]
    }
    
    let reader = mockReader();
    reader.readOffsetIndex = () => Promise.reject('SHOULDNT GET HIT');
    reader.readColumnIndex = () => Promise.reject('SHOULDNT GET HIT');
    let pathStreamer = new PathStreamer(spec, [reader]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 0);
  });
  
  it('reader statistics winnowing - value hits', async function() {
    let spec = {
      filter: [
        { path: 'quantity', value: 21, index: true }
      ]
    }
    
    let pathStreamer = new PathStreamer(spec, [mockReader()]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 2);
    assert.equal(results[0].rowGroup.num_rows, 6);
    assert.equal(results[0].lowIndex, 0);
    assert.equal(results[0].highIndex, 3);
    assert.equal(results[1].rowGroup.num_rows, 5);
    assert.equal(results[1].lowIndex, 3);
    assert.equal(results[1].highIndex, 4);
  });


  it('can find page values', async function() {
    let spec = {
      filter: [
        { path: 'quantity', value: 25 }
      ]
    }
    
    let pathStreamer = new PathStreamer(spec, [mockReader()]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 2);
    assert.equal(results[0].rowGroup.num_rows, 6);
    assert.equal(results[0].lowIndex, 1);
    assert.equal(results[0].highIndex, 1);
    assert.equal(results[1].rowGroup.num_rows, 6);
    assert.equal(results[1].lowIndex, 5);
    assert.equal(results[1].highIndex, 5);
  });


  it('can find page values', async function() {
    let spec = {
      filter: [
        { path: 'quantity', min: 20, max: 25 }
      ]
    }
    
    let pathStreamer = new PathStreamer(spec, [mockReader()]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 3);
    assert.equal(results[0].rowGroup.num_rows, 6);
    assert.equal(results[0].lowIndex, 0);
    assert.equal(results[0].highIndex, 1);
    assert.equal(results[1].rowGroup.num_rows, 6);
    assert.equal(results[1].lowIndex, 5);
    assert.equal(results[1].highIndex, 5);
    assert.equal(results[2].rowGroup.num_rows, 5);
    assert.equal(results[2].lowIndex, 0);
    assert.equal(results[2].highIndex, 0);
  });

  it('doesnt read page if it doesnt have to', async function() {
    let reader = mockReader();
    reader.metadata.row_groups = [reader.metadata.row_groups[1]];
    reader.pageData = () => Promise.reject('SHOULDNT GET HIT');

    let spec = {
      filter: [
        { path: 'quantity', value: 20 }
      ]
    }
    
    let pathStreamer = new PathStreamer(spec, [reader]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 1);
    assert.equal(results[0].rowGroup.num_rows, 5);
    assert.equal(results[0].lowIndex, 0);
    assert.equal(results[0].highIndex, 0);
  });

  it('doesnt read page if it doesnt have to for ranges', async function() {
    let reader = mockReader();
    reader.pageData = () => Promise.reject('SHOULDNT GET HIT');

    let spec = {
      filter: [
        { path: 'quantity', min: 0, max: 100 }
      ]
    }
    
    let pathStreamer = new PathStreamer(spec, [reader]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 2);
    assert.equal(results[0].rowGroup.num_rows, 6);
    assert.equal(results[0].lowIndex, 0);
    assert.equal(results[0].highIndex, 5);
    assert.equal(results[1].rowGroup.num_rows, 5);
    assert.equal(results[1].lowIndex, 0);
    assert.equal(results[1].highIndex, 4);
  });


  it('can load fields', async function() {
    let spec = {
      filter: [
        { path: 'quantity', value: 25 }
      ],
      fields: [
        { path: 'quantity' },
        { path: 'name' },
      ]
    }
    
    let pathStreamer = new PathStreamer(spec, [mockReader()]);
    let results = await pathStreamer.stream.promise();

    assert.equal(results.length, 2);
    assert.deepEqual(results[0], {quantity: 25, name: 'dallas'});
    assert.deepEqual(results[1], {quantity: 25, name: 'miles' });
  });

});