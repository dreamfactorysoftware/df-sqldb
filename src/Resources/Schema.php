<?php
namespace DreamFactory\Core\SqlDb\Resources;

use DreamFactory\Core\Database\Resources\DbSchemaResource;
use DreamFactory\Core\SqlDb\Components\TableDescriber;

class Schema extends DbSchemaResource
{
    //*************************************************************************
    //	Traits
    //*************************************************************************

    use TableDescriber;

    //*************************************************************************
    //	Methods
    //*************************************************************************

    public static function getApiDocInfo($service, array $resource = [])
    {
        $base = parent::getApiDocInfo($service, $resource);

        $base['definitions'] = array_merge($base['definitions'], static::getApiDocCommonModels());

        return $base;
    }
}