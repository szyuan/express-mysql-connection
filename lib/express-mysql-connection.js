var _mysql,
    _dbConfig,
    _connection, // This is used as a singleton in a single connection strategy
    _pool, // Pool singleton
    _middlewareFn; 


module.exports = function (mysql, dbConfig, strategy) {

    if (null == mysql) throw new Error('Missing mysql module param!');
    if (null == dbConfig) throw new Error('Missing dbConfig module param!');
    if (null == strategy) strategy = 'single';

    // Setting _mysql module ref
    _mysql = mysql;

    // Setting _dbConfig ref
    _dbConfig = dbConfig;

    // Configuring strategies
    switch (strategy) {
        case 'single':
            // Creating single connection instance
            _connection = _mysql.createConnection(dbConfig);
            handleDisconnect(dbConfig);
            // single connection strategy
            // 单链接策略时返回的中间件 * * * * * * * * * * * * * * * *
            _middlewareFn = function(req, res, next) {
                req.getConnection = function (callback) {
                    callback(null, _connection);
                }
                next();
            };
            break;

        case 'pool':
            // Creating pool instance
            _pool = _mysql.createPool(dbConfig);
            // pool
            // 连接池策略时返回的中间件 * * * * * * * * * * * * * * * *
            _middlewareFn = function(req, res, next) {
                var poolConn = null;
                // Returning cached connection from a pool, caching is on request level
                if(req.__expressMysqlConnectionCache__) {
                    req.getConnection = function (callback) {
                        pollConn = req.__expressMysqlConnectionCache__;
                        callback(null, requestConn);
                    }
                }else {
                // Getting connection from a pool
                    req.getConnection = function (callback) {
                        _pool.getConnection(function (err, connection) {
                            if (err) return callback(err);
                            poolConnection = connection;
                            callback(null, poolConnection);
                        });
                    }
                }
                closeConnection(res, poolConn);
                next();
            }
            break;
        case 'request':
            // request
            // 请求时创建连接，请求结束后自动释放  * * * * * * * * * * * *
            _middlewareFn = function(req, res, next) {
                var requestConn = null;
                // Returning cached connection, caching is on request level
                if(req.__expressMysqlConnectionCache__) {
                    req.getConnection = function(callback) {
                        requestConn = req.__expressMysqlConnectionCache__;
                        callback(null, requestConn);
                    }
                }else {
                    req.getConnection = function(callback) {
                        requestConn = _mysql.createConnection(dbConfig);
                        requestConn.connect(function (err) {
                            if (err) return callback(err);
                            req.__expressMysqlConnectionCache__ = requestConn;
                            callback(null, requestConn);
                        });
                    }
                }
                closeConnection(res, null, requestConn);
                next();
            };
            break;
        default:
            throw new Error('Not supported connection strategy!');
    }
    return _middlewareFn;
}

function handleDisconnect() {
    _connection = _mysql.createConnection(_dbConfig);

    _connection.connect(function (err) {
        if (err) {
            console.log('error when connecting to db:', err);
            setTimeout(handleDisconnect, 2000);
        }
    });

    _connection.on('error', function (err) {
        console.log('db error', err);
        if (err.code === 'PROTOCOL_CONNECTION_LOST') {
            handleDisconnect();
        } else {
            throw err;
        }
    });
}

function closeConnection(res, poolConnection, requestConnection) {
    // Request closed unexpectedly.
	res.on("close", closeHandler);
    // Finish
	res.on("finish", closeHandler);

    function closeHandler() {
		if (poolConnection) poolConnection.release();
		if (requestConnection) requestConnection.end();
    }
}
