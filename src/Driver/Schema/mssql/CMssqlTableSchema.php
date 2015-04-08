<?php
/**
 * CMssqlTableSchema class file.
 *
 * @author    Qiang Xue <qiang.xue@gmail.com>
 * @author    Christophe Boulain <Christophe.Boulain@gmail.com>
 * @link      http://www.yiiframework.com/
 * @copyright Copyright &copy; 2008-2011 Yii Software LLC
 * @license   http://www.yiiframework.com/license/
 */

/**
 * CMssqlTableSchema represents the metadata for a MSSQL table.
 *
 * @author  Qiang Xue <qiang.xue@gmail.com>
 * @author  Christophe Boulain <Christophe.Boulain@gmail.com>
 * @package system.db.schema.mssql
 */
namespace DreamFactory\Rave\SqlDb\Driver\Schema\Mssql;

use DreamFactory\Rave\SqlDb\Driver\Schema\CDbTableSchema;

class CMssqlTableSchema extends CDbTableSchema
{
    /**
     * @var string name of the catalog (database) that this table belongs to.
     * Defaults to null, meaning no schema (or the current database).
     */
    public $catalogName;
}
