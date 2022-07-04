<?php

class SqlDbConfigTest extends \DreamFactory\Core\Database\Testing\DbServiceConfigTestCase
{
    protected $types = ['pgsql', 'sqlite', 'alloydb'];
}