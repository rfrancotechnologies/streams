# Why this library

A service typically need to atomically update the database and publish messages/events. To solve this, the application that uses a relational database inserts messages/events into an outbox table (in this case, in the internal change tracking table associated to the table) as part of the local transaction.  A separate Message Relay process publishes the events inserted into database to a message broker. This solution is called the `Outbox pattern`.

This library helps applications to use Change Tracking to implement the `Outbox pattern`.

## What is Change Tracking

`Change tracking` is a lightweight solution that provides an efficient change tracking mechanism for applications.
Applications that have to synchronize data with an instance of the SQL Server Database Engine must be able to query for changes
and apply these changes to another service or data store.

## How Change Tracking Works

To track changes, change tracking must first be enabled for the database and then enabled for the tables that you want to track within that database. The table definition does not have to be changed in any way, and no triggers are created.

After change tracking is configured for a table, any DML statement that affects rows in the table will cause change tracking information for each modified row to be recorded. To query for the rows that have changed and to obtain information about the changes, you can use change tracking functions.

The values of the primary key column is only information from the tracked table that is recorded with the change information. These values identify the rows that have been changed. To obtain the latest data for those rows, an application can use the primary key column values to join the source table with the tracked table.

Information about the change that was made to each row can also be obtained by using change tracking. For example, the type of DML operation that caused the change (insert, update, or delete) or the columns that were changed as part of an update operation.

### Obtaining Initial Data and Changes detection

Before an application can obtain changes for the first time, the application must send a query to obtain the initial data and the synchronization version.

To obtain the changed rows for a table and information about the changes, use the current synchronization version to retrieve new changes since the last synchronization.

### Validating the Last Synchronized Version

Information about changes is maintained for a limited time. The length of time is controlled by the CHANGE_RETENTION parameter that can be specified as part of the ALTER DATABASE.
Be aware that the time specified for CHANGE_RETENTION determines how frequently all applications must request changes from the database. If an application has a value for last_synchronization_version that is older than the minimum valid synchronization version for a table, that application cannot perform valid change enumeration.

## How to use this library

### Configuration
