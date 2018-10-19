#!/usr/bin/env python
# -*- coding:utf-8 -*-


class HiveType:
    @staticmethod
    def change_type(ctype):
        octype = ctype
        ctype = ctype.lower().strip()
        if ctype in ("varchar", "char"):
            octype = "string"
        if ctype in ("datetime",):
            octype = "timestamp"
        if ctype in ("text", "longtext", "mediumtext"):
            octype = "string"
        if ctype == "time":
            octype = "string"
        if ctype == "text":
            octype = "string"
        if ctype in ("long", "int"):
            octype = "bigint"
        if ctype in ("smallint", "mediumint", "tinyint"):
            octype = "int"
        if ctype in ("decimal", "float"):
            octype = "double"
        if ctype == "array":
            octype = "string"
        if ctype == "enum":
            octype = "string"
        if ctype == "bit":
            octype = "string"
        if ctype == "date":
            octype = "date"
        print "类型转换:", str(ctype) + " -> " + str(octype)
        return octype

    @staticmethod
    def change_mongo_type(ctype):
        octype = ctype
        ctype = ctype.lower().strip()
        if ctype in ("varchar", "char"):
            octype = "string"
        if ctype in ("datetime",):
            octype = "timestamp"
        if ctype in ("text", "longtext"):
            octype = "string"
        if ctype == "time":
            octype = "string"
        if ctype == "text":
            octype = "string"
        if ctype in ("long", "int"):
            octype = "bigint"
        if ctype in ("smallint", "mediumint", "tinyint"):
            octype = "int"
        if ctype in ("decimal", "float"):
            octype = "double"
        if ctype == "date":
            octype = "timestamp"
        if ctype == "array":
            octype = "string"
        print "类型转换:", str(ctype) + " -> " + str(octype)
        return octype

    @staticmethod
    def change_hiveserver2_type(ctype):
        # STRING_TYPE/BIGINT_TYPE etc
        octype = (ctype[:ctype.rfind("_TYPE")]).lower()
        # 特殊类型处理
        if ctype == 'DECIMAL_TYPE':
            octype = "double"
        print "类型转换:", str(ctype) + " -> " + str(octype)
        return octype


    @staticmethod
    def key_words(cname):
        cname = cname.upper()
        keys = "ADD, ADMIN, AFTER, ANALYZE, ARCHIVE, ASC, BEFORE, BUCKET, BUCKETS," \
                "CASCADE, CHANGE, CLUSTER, CLUSTERED, CLUSTERSTATUS, COLLECTION," \
                "COLUMNS, COMMENT, COMPACT, COMPACTIONS, COMPUTE, CONCATENATE," \
                "CONTINUE, DATA, DATABASES, DATETIME, DAY, DBPROPERTIES," \
                "DEFERRED, DEFINED, DELIMITED, DEPENDENCY, DESC, DIRECTORIES," \
                "DIRECTORY, DISABLE, DISTRIBUTE, ELEM_TYPE, ENABLE," \
                "ESCAPED, EXCLUSIVE, EXPLAIN, EXPORT, FIELDS, FILE," \
                "FILEFORMAT, FIRST, FORMAT, FORMATTED, FUNCTIONS," \
                "HOLD_DDLTIME, HOUR, IDXPROPERTIES, IGNORE, INDEX, INDEXES, INPATH," \
                "INPUTDRIVER, INPUTFORMAT, ITEMS, JAR, KEYS, KEY_TYPE, LIMIT, LINES," \
                "LOAD, LOCATION, LOCK, LOCKS, LOGICAL, LONG, MAPJOIN, MATERIALIZED," \
                "METADATA, MINUS, MINUTE, MONTH, MSCK, NOSCAN, NO_DROP, OFFLINE, OPTION," \
                "OUTPUTDRIVER, OUTPUTFORMAT, OVERWRITE, OWNER, PARTITIONED, PARTITIONS," \
                "PLUS, PRETTY, PRINCIPALS, PROTECTION, PURGE, READ, READONLY, REBUILD, RECORDREADER," \
                "RECORDWRITER, REGEXP, RELOAD, RENAME, REPAIR, REPLACE, REPLICATION, RESTRICT," \
                "REWRITE,RLIKE, ROLE, ROLES, SCHEMA, SCHEMAS, SECOND, SEMI, SERDE, SERDEPROPERTIES,SERVER," \
                "SETS, SHARED, SHOW, SHOW_DATABASE, SKEWED, SORT, SORTED, SSL, STATISTICS," \
                "STORED, STREAMTABLE, STRING, STRUCT, TABLES, TBLPROPERTIES, TEMPORARY, TERMINATED," \
                "TINYINT, TOUCH, TRANSACTIONS, UNARCHIVE, UNDO, UNIONTYPE, UNLOCK, UNSET, UNSIGNED," \
                "URI, USE, UTC, UTCTIMESTAMP, VALUE_TYPE, VIEW, WHILE, YEAR," \
                "ALL, ALTER, AND, ARRAY, AS, AUTHORIZATION, BETWEEN, BIGINT, BINARY," \
                "BOOLEAN, BOTH, BY, CASE, CAST, CHAR, COLUMN, CONF, CREATE, CROSS," \
                "CUBE, CURRENT, CURRENT_DATE, CURRENT_TIMESTAMP, CURSOR, DATABASE," \
                "DATE, DECIMAL, DELETE, DESCRIBE, DISTINCT, DOUBLE, DROP, ELSE, END," \
                "EXCHANGE, EXISTS, EXTENDED, EXTERNAL, FALSE, FETCH, FLOAT, FOLLOWING," \
                "FOR, FROM, FULL, FUNCTION, GRANT, GROUP, GROUPING, HAVING, IF, IMPORT," \
                "IN, INNER, INSERT, INT, INTERSECT, INTERVAL, INTO, IS, JOIN, LATERAL, LEFT," \
                "LESS, LIKE, LOCAL, MACRO, MAP, MORE, NONE, NOT, NULL, OF, ON, OR, ORDER,OUT," \
                "OUTER, OVER, PARTIALSCAN, PARTITION, PERCENT, PRECEDING, PRESERVE, PROCEDURE," \
                "RANGE,READS, REDUCE, REVOKE, RIGHT, ROLLUP, ROW, ROWS, SELECT, SET, SMALLINT, TABLE," \
                "TABLESAMPLE,THEN, TIMESTAMP, TO, TRANSFORM, TRIGGER, TRUE, TRUNCATE, UNBOUNDED, UNION," \
                "UNIQUEJOIN,UPDATE, USER, USING, UTC_TMESTAMP, VALUES, VARCHAR, WHEN, WHERE, WINDOW, WITH"
        keys_set = set()
        for key in keys.split(","):
            keys_set.add(key.strip())
        if cname in keys_set:
            return True
        else:
            return False

if __name__ == '__main__':
    print HiveType.change_type("time")
