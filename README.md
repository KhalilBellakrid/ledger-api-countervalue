## Node.js reference implementation of ledger-api-countervalue

## Setup

* Node.js
* yarn
* mongodb

## Install dependencies

```
yarn
```
## Using MongoDB

### Install MongoDB on MacOS
Tap package:
```
brew tap mongodb/brew
```
then install it:
```
brew install mongodb-community@4.0
```
### Setup MongoDB
First, create folder which will contain data:
```
mkdir data
```
Second, start launching the `mongod`:

```
mongod --dbpath ./data
```
By default it will be served on `localhost` on `27017` port, if you want to specify different port, you can use `--port` to specify it.

## Using PostgreSQL
### Install PostgreSQL on MacOS
Install package:
```
brew install postgres
```
### Setup PostgreSQL
```
initdb -D <db_name>
pg_ctl start -D <db_name> -l <log_file_name> -o "-i -h <host> -p <port>"
createdb -h <host> -p <port> <db_name>
```
## Building for dev

```
yarn watch
```

## Building for prod

```
yarn build
```

## Run the HTTP Server

```
yarn start
```

## Run the countervalue sync

```
yarn start-sync
```
