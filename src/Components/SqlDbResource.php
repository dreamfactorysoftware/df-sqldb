<?php
namespace DreamFactory\Core\SqlDb\Components;

use DreamFactory\Core\Contracts\RequestHandlerInterface;
use DreamFactory\Core\SqlDbCore\Connection;
use DreamFactory\Core\SqlDb\Services\SqlDb;

trait SqlDbResource
{
    //*************************************************************************
    //	Members
    //*************************************************************************

    /**
     * @var Connection
     */
    protected $dbConn = null;

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
    }
}