const ValueFilter = require('./valueFilter');
const IndexFilter = require('./indexFilter');
let MultiItemFilter = null;

module.exports = spec => {
  if (!MultiItemFilter) {
    MultiItemFilter = require('./multiItemFilter'); // handle circular dependency
  }

  if (spec.index) {
    if (spec.value !== undefined) {
      return new IndexFilter.FilterValueIndexPhase(spec);
    }
    else if (spec.min !== undefined || spec.max !== undefined) {
      return new IndexFilter.FilterRangeIndexPhase(spec);
    }
    else {
      return new IndexFilter.LoadIndex(spec);
    }
  }
  else if (spec.offset) {
    return new ValueFilter.LoadOffset(spec);
  }
  else if (spec.value !== undefined) {
    return new ValueFilter.FilterValuePhase(spec);
  }
  else if (spec.min !== undefined || spec.max !== undefined) {
    return new ValueFilter.FilterRangePhase(spec);
  }
  else if (spec.or) {
    return new MultiItemFilter.FilterOrPhase(spec.or);
  }
  else if (Array.isArray(spec)) {
    return new MultiItemFilter.FilterAndPhase(spec);
  }
  else if (spec.and) {
    return new MultiItemFilter.FilterAndPhase(spec.and);
  }
}
