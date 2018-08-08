exports.getVersionAsInteger = version => (version instanceof Date) ? version.getTime() : version;

// Manually increment existing doc version number by 1 (if not null) to enable ES to perform deletion of the
// versioned document (since we don't get a fullDocument from Mongo for 'delete' change events)
exports.incrementVersionForDeletion = version => Number.isInteger(version) ? version++ : null;
