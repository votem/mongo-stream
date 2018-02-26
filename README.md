# Mongo Stream

Mongo Stream is a Node.js project that aims to make use of Mongo 3.6+ change-streams
to synchronize data between Mongodb and ElasticSearch

## Prerequisites

* Node.js (tested with version 8)
* Mongodb 3.6+
* Elasticsearch 2.4.5, 5.x+ (Compatibility for 6 is a future goal)

## Setup

```
git clone https://github.com/everyone-counts/mongo-stream.git

npm install
```

## Usage

To use with default configuation:
```
npm start
```

To use with a custom config file:
```
./server.js -c <path-to-config-file>
```

## Contributing

Feel free to submit pull requests. If you'd like to contribute towards closing an open issue please leave a comment with your solution proposal in the comment section of the issue.  

## Credits

This software was developed as an internal project at Everyone Counts, Inc.

Everyone Counts provides elegantly simple software and exceptional services to upgrade
the election process. The eLect brand offers unrivaled web-based voting products including
Voter Registration, Online Voting, and Remote Accessible Voting. The product suite
is designed to address pain points facing election officials today, including security,
accessibility, cost, and sustainability.

For more information, visit https://everyonecounts.com.
