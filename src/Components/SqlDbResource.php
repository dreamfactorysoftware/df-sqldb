<?php
namespace DreamFactory\Core\SqlDb\Components;

use DreamFactory\Core\Contracts\RequestHandlerInterface;
use DreamFactory\Core\Contracts\SchemaInterface;
use DreamFactory\Core\SqlDb\Services\SqlDb;
use Illuminate\Database\ConnectionInterface;

trait SqlDbResource
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var ConnectionInterface
     */
    protected $dbConn = null;
    /**
     * @var SchemaInterface
     */
    protected $schema = null;


    //*************************************************************************
    //	Methods
    //*************************************************************************

    /**
     * @param RequestHandlerInterface $parent
     */
    public function setParent(RequestHandlerInterface $parent)
    {
        parent::setParent($parent);

        /** @var SqlDb $parent */
        $this->dbConn = $parent->getConnection();

        /** @var SqlDb $parent */
        $this->schema = $parent->getSchemaExtension($this->dbConn);
    }
}