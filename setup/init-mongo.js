// setup/init-mongo.js

// Switch to nomba database
db = db.getSiblingDB('nomba');

// Create users collection
db.createCollection('users');

// Create indexes for performance
db.users.createIndex({ "_Uid": 1 }, { unique: true });
db.users.createIndex({ "state": 1 });
db.users.createIndex({ "occupation": 1 });

print('MongoDB initialized successfully');
print('  - Database: nomba');
print('  - Collection: users');
print('  - Indexes: Uid (unique), state, occupation');