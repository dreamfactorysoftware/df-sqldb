<?php
/**
 * This file is part of the DreamFactory Rave(tm)
 *
 * DreamFactory Rave(tm) <http://github.com/dreamfactorysoftware/rave>
 * Copyright 2012-2014 DreamFactory Software, Inc. <support@dreamfactory.com>
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace DreamFactory\Rave\SqlDb\Models;

use DreamFactory\Library\Utility\ArrayUtils;
use DreamFactory\Rave\Components\RequireExtensions;
use DreamFactory\Rave\Exceptions\BadRequestException;
use DreamFactory\Rave\Models\BaseServiceConfigModel;
use DreamFactory\Rave\SqlDbCore\Connection;

/**
 * SqlDbConfig
 *
 * @property integer $service_id
 * @property string  $dsn
 * @property string  $username
 * @property string  $password
 * @property string  $db
 * @property string  $options
 * @property string  $attributes
 * @method static \Illuminate\Database\Query\Builder|SqlDbConfig whereServiceId( $value )
 */
class SqlDbConfig extends BaseServiceConfigModel
{
    use RequireExtensions;

    protected $table = 'sql_db_config';

    protected $fillable = [ 'service_id', 'dsn', 'username', 'password', 'db', 'options', 'attributes' ];

    protected $casts = [ 'options' => 'array', 'attributes' => 'array' ];

    protected $encrypted = [ 'username', 'password' ];

    public static function validateConfig( $config )
    {
        if ( null === ( $dsn = ArrayUtils::get( $config, 'dsn', null, true ) ) )
        {
            throw new BadRequestException( 'Database connection string (DSN) can not be empty.' );
        }

        $driver = Connection::getDriverFromDSN($dsn);
        Connection::requireDriver($driver);

        return true;
    }
}