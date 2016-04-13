<?php

namespace DreamFactory\Core\SqlDb\Services;

use DreamFactory\Core\Components\DbSchemaExtras;
use DreamFactory\Core\Contracts\CacheInterface;
use DreamFactory\Core\Contracts\DbExtrasInterface;
use DreamFactory\Core\Contracts\SchemaInterface;
use DreamFactory\Core\Database\ConnectionExtension;
use DreamFactory\Core\Database\TableSchema;
use DreamFactory\Core\Exceptions\InternalServerErrorException;
use DreamFactory\Core\Services\BaseDbService;
use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\SqlDb\Resources\StoredFunction;
use DreamFactory\Core\SqlDb\Resources\StoredProcedure;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\Utility\Session;
use DreamFactory\Library\Utility\Scalar;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\DatabaseManager;

/**
 * Class SqlDb
 *
 * @package DreamFactory\Core\SqlDb\Services
 */
class SqlDb extends BaseDbService implements CacheInterface, DbExtrasInterface
{
    use ConnectionExtension, DbSchemaExtras;

    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var ConnectionInterface
     */
    protected $dbConn;

    /**
     * @var SchemaInterface
     */
    protected $schema;

    /**
     * @var array
     */
    protected static $resources = [
        Schema::RESOURCE_NAME          => [
            'name'       => Schema::RESOURCE_NAME,
            'class_name' => Schema::class,
            'label'      => 'Schema',
        ],
        Table::RESOURCE_NAME           => [
            'name'       => Table::RESOURCE_NAME,
            'class_name' => Table::class,
            'label'      => 'Tables',
        ],
        StoredProcedure::RESOURCE_NAME => [
            'name'       => StoredProcedure::RESOURCE_NAME,
            'class_name' => StoredProcedure::class,
            'label'      => 'Stored Procedures',
        ],
        StoredFunction::RESOURCE_NAME  => [
            'name'       => StoredFunction::RESOURCE_NAME,
            'class_name' => StoredFunction::class,
            'label'      => 'Stored Functions',
        ],
    ];

    //*************************************************************************
    //	Methods
    //*************************************************************************

    public static function adaptConfig(array &$config)
    {
        $driver = isset($config['driver']) ? $config['driver'] : null;
        // replace old driver names or setup environment
        switch ($driver) {
            case 'dblib':
            case 'sqlsrv':
                $config['driver'] = 'sqlsrv';
                $driver = 'sqlsrv';
                if (in_array('dblib', \PDO::getAvailableDrivers())) {
                    if (null !== $dumpLocation = config('df.db.freetds.dump')) {
                        if (!putenv("TDSDUMP=$dumpLocation")) {
                            \Log::alert('Could not write environment variable for TDSDUMP location.');
                        }
                    }
                    if (null !== $dumpConfLocation = config('df.db.freetds.dumpconfig')) {
                        if (!putenv("TDSDUMPCONFIG=$dumpConfLocation")) {
                            \Log::alert('Could not write environment variable for TDSDUMPCONFIG location.');
                        }
                    }
                    if (null !== $confLocation = config('df.db.freetds.sqlsrv')) {
                        if (!putenv("FREETDSCONF=$confLocation")) {
                            \Log::alert('Could not write environment variable for FREETDSCONF location.');
                        }
                    }
                }
                break;
            case 'sqlanywhere':
                if (in_array('dblib', \PDO::getAvailableDrivers())) {
                    if (null !== $dumpLocation = config('df.db.freetds.dump')) {
                        if (!putenv("TDSDUMP=$dumpLocation")) {
                            \Log::alert('Could not write environment variable for TDSDUMP location.');
                        }
                    }
                    if (null !== $dumpConfLocation = config('df.db.freetds.dumpconfig')) {
                        if (!putenv("TDSDUMPCONFIG=$dumpConfLocation")) {
                            \Log::alert('Could not write environment variable for TDSDUMPCONFIG location.');
                        }
                    }
                    if (null !== $confLocation = config('df.db.freetds.sqlanywhere')) {
                        if (!putenv("FREETDSCONF=$confLocation")) {
                            \Log::alert('Could not write environment variable for FREETDSCONF location.');
                        }
                    }
                }
                break;
            case 'oci':
                $config['driver'] = 'oracle';
                $driver = 'oracle';
                break;
        }
        $dsn = isset($config['dsn']) ? $config['dsn'] : null;
        if (!empty($dsn)) {
            // default PDO DSN pieces
            $dsn = str_replace(' ', '', $dsn);
            if ('oracle' !== $driver) { // see below
                if (!isset($config['port']) && (false !== ($pos = strpos($dsn, 'port=')))) {
                    $temp = substr($dsn, $pos + 5);
                    $config['port'] = (false !== $pos = strpos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                }
                if (!isset($config['host']) && (false !== ($pos = strpos($dsn, 'host=')))) {
                    $temp = substr($dsn, $pos + 5);
                    $host = (false !== $pos = stripos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                    if (!isset($config['port']) && (false !== ($pos = stripos($host, ':')))) {
                        $temp = substr($host, $pos + 1);
                        $host = substr($host, 0, $pos);
                        $config['port'] = (false !== $pos = stripos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                    }
                    $config['host'] = $host;
                }
                if (!isset($config['database']) && (false !== ($pos = strpos($dsn, 'dbname=')))) {
                    $temp = substr($dsn, $pos + 7);
                    $config['database'] = (false !== $pos = strpos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                }
                if (!isset($config['charset'])) {
                    if (false !== ($pos = strpos($dsn, 'charset='))) {
                        $temp = substr($dsn, $pos + 8);
                        $config['charset'] = (false !== $pos = strpos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                    } else {
                        $config['charset'] = 'utf8';
                    }
                }
            }

            // specials
            switch ($driver) {
                case 'sqlite':
                    if (!isset($config['database'])) {
                        $file = substr($dsn, 7);
                        if (false === strpos($file, DIRECTORY_SEPARATOR)) {
                            // no directories involved, store it where we want to store it
                            $storage = config('df.db.sqlite_storage');
                            if (!is_dir($storage)) {
                                // Attempt
                                @mkdir($storage);
                            }
                            if (!is_dir($storage)) {
                                logger('Failed to access storage path ' . $storage);
                                throw new InternalServerErrorException('Failed to access storage path.');
                            }

                            $file = rtrim($storage, DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR . $file;
                            if (!file_exists($file)) {
                                @touch($file);
                            }
                        }
                        $config['database'] = $file;
                    }
                    break;
                case 'sqlsrv':
                    // SQL Server native driver specifics
                    if (!isset($config['host']) && (false !== ($pos = stripos($dsn, 'Server=')))) {
                        $temp = substr($dsn, $pos + 7);
                        $host = (false !== $pos = stripos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                        if (!isset($config['port']) && (false !== ($pos = stripos($host, ',')))) {
                            $temp = substr($host, $pos + 1);
                            $host = substr($host, 0, $pos);
                            $config['port'] = (false !== $pos = stripos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                        }
                        $config['host'] = $host;
                    }
                    if (!isset($config['database']) && (false !== ($pos = stripos($dsn, 'Database=')))) {
                        $temp = substr($dsn, $pos + 9);
                        $config['database'] = (false !== $pos = stripos($temp, ';')) ? substr($temp, 0, $pos) : $temp;
                    }
                    break;
                case 'oracle':
                    // traditional connection string uses (), reset find
                    if (!isset($config['host']) && (false !== ($pos = stripos($dsn, 'host=')))) {
                        $temp = substr($dsn, $pos + 5);
                        $config['host'] = (false !== $pos = stripos($temp, ')')) ? substr($temp, 0, $pos) : $temp;
                    }
                    if (!isset($config['port']) && (false !== ($pos = stripos($dsn, 'port=')))) {
                        $temp = substr($dsn, $pos + 5);
                        $config['port'] = (false !== $pos = stripos($temp, ')')) ? substr($temp, 0, $pos) : $temp;
                    }
                    if (!isset($config['database']) && (false !== ($pos = stripos($dsn, 'sid=')))) {
                        $temp = substr($dsn, $pos + 4);
                        $config['database'] = (false !== $pos = stripos($temp, ')')) ? substr($temp, 0, $pos) : $temp;
                    }
                    break;
                default:
                    break;
            }
        }

        if (!isset($config['collation'])) {
            $config['collation'] = 'utf8_unicode_ci';
        }

        // must be there
        if (!array_key_exists('database', $config)) {
            $config['database'] = null;
        }

        // must be there
        if (!array_key_exists('prefix', $config)) {
            $config['prefix'] = null;
        }

        // laravel database config requires options to be [], not null
        if (array_key_exists('options', $config) && is_null($config['options'])) {
            $config['options'] = [];
        }
    }

    /**
     * Create a new SqlDbSvc
     *
     * @param array $settings
     *
     * @throws \InvalidArgumentException
     * @throws \Exception
     */
    public function __construct($settings = [])
    {
        parent::__construct($settings);

        $config = array_get($settings, 'config', []);
        Session::replaceLookups($config, true);

        static::adaptConfig($config);

        // add config to global for reuse, todo check existence and update?
        config(['database.connections.service.' . $this->name => $config]);
        /** @type DatabaseManager $db */
        $db = app('db');
        $this->dbConn = $db->connection('service.' . $this->name);
        $this->initStatements();

        $this->schema = $this->getSchema($this->dbConn);
        $this->schema->setCache($this);
        $this->schema->setExtraStore($this);

        $defaultSchemaOnly = Scalar::boolval(array_get($config, 'default_schema_only'));
        $this->schema->setDefaultSchemaOnly($defaultSchemaOnly);
    }

    /**
     * Object destructor
     */
    public function __destruct()
    {
        if (isset($this->dbConn)) {
            try {
                $this->dbConn = null;
            } catch (\Exception $ex) {
                error_log("Failed to disconnect from database.\n{$ex->getMessage()}");
            }
        }
    }

    /**
     * @throws \Exception
     * @return ConnectionInterface
     */
    public function getConnection()
    {
        if (!isset($this->dbConn)) {
            throw new InternalServerErrorException('Database connection has not been initialized.');
        }

        return $this->dbConn;
    }

    protected function initStatements($statements = [])
    {
        if (is_string($statements)) {
            $statements = [$statements];
        } elseif (!is_array($statements)) {
            $statements = [];
        }

        switch ($this->dbConn->getDriverName()) {
            case 'sqlite':
                array_unshift($statements, 'PRAGMA foreign_keys=1');
                break;
            case 'sqlsrv':
                // These are on by default for sqlsrv driver, but not dblib.
                // Also, can't use 'SET ANSI_DEFAULTS ON', seems to return false positives for DROP TABLE etc. todo
                array_unshift($statements, 'SET QUOTED_IDENTIFIER ON;');
                array_unshift($statements, 'SET ANSI_WARNINGS ON;');
                array_unshift($statements, 'SET ANSI_NULLS ON;');
                break;
        }
        foreach ($statements as $statement) {
            $this->dbConn->statement($statement);
        }
    }

    /**
     * @param string|null $schema
     * @param bool        $refresh
     * @param bool        $use_alias
     *
     * @return TableSchema[]
     * @throws \Exception
     */
    public function getTableNames($schema = null, $refresh = false, $use_alias = false)
    {
        /** @type TableSchema[] $tables */
        $tables = $this->schema->getTableNames($schema, true, $refresh);
        if ($use_alias) {
            $temp = []; // reassign index to alias
            foreach ($tables as $table) {
                $temp[strtolower($table->getName(true))] = $table;
            }

            return $temp;
        }

        return $tables;
    }

    public function refreshTableCache()
    {
        $this->schema->refresh();
    }
}