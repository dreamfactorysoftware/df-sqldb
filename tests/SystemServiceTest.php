<?php
/**
 * This file is part of the DreamFactory Rave(tm)
 *
 * DreamFactory Rave(tm) <http://github.com/dreamfactorysoftware/rave>
 * Copyright 2012-2014 DreamFactory Software, Inc. <support@dreamfactory.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use DreamFactory\Library\Utility\Enums\Verbs;

class SystemServiceTest extends \DreamFactory\Rave\Testing\TestCase
{
    protected static $staged = false;

    public function stage()
    {
        parent::stage();

        Artisan::call( 'migrate', [ '--path' => 'vendor/dreamfactory/rave-sqldb/database/migrations/']);
        Artisan::call( 'db:seed', [ '--class' => 'DreamFactory\\Rave\\SqlDb\\Database\\Seeds\\DatabaseSeeder' ] );
    }

    public function tearDown()
    {
        $this->deleteDbService(1);
        $this->deleteDbService(2);
        $this->deleteDbService(3);

        parent::tearDown();
    }


    /************************************************
     * Testing GET
     ************************************************/

    public function testGET()
    {
        $rs = $this->call( Verbs::GET, $this->prefix . "/system" );
        $this->assertContains( '"resource":', $rs->getContent() );
    }

    public function testGETService()
    {
        $rs = $this->call( Verbs::GET, $this->prefix . "/system/service" );
        $this->assertContains( '"record":', $rs->getContent() );
    }

    public function testGETServiceById()
    {
        $id = $this->createDbService(1);
        $rs = $this->call( Verbs::GET, $this->prefix . "/system/service/".$id );
        $data = json_decode( $rs->getContent(), true );
        $this->assertTrue( $data['id'] == $id );
    }

    public function testGETServiceByIds()
    {
        $id1 = $this->createDbService(1);
        $id2 = $this->createDbService(2);
        $id3 = $this->createDbService(3);

        $rs = $this->call( Verbs::GET, $this->prefix . "/system/service?ids=$id1,$id2,$id3" );
        $data = json_decode( $rs->getContent(), true );
        $ids = implode( ",", array_column( $data['record'], 'id' ) );

        $this->assertTrue( $ids == "$id1,$id2,$id3" );
    }

    public function testGETOverPOSTTunnel()
    {
        $id1 = $this->createDbService(1);
        $id2 = $this->createDbService(2);

        $payload = '[{"id":'.$id1.'},{"id":'.$id2.'}]';

        $rs = $this->call( Verbs::POST, $this->prefix . "/system/service", [ ], [ ], [ ], [ "HTTP_X_HTTP_METHOD" => Verbs::GET ], $payload );

        $data = json_decode( $rs->getContent(), true );
        $label = implode( ",", array_column( $data['record'], 'label' ) );

        $this->assertEquals( "Database1,Database2", $label );
    }

    public function testResourceNotFound()
    {
        $this->call( Verbs::GET, $this->prefix . "/system/foo" );
        $this->assertResponseStatus( 404 );
    }

    /************************************************
     * Testing POST
     ************************************************/

    public function testPOSTService()
    {
        $payload = '{"record":[{"name":"db9","label":"Database","description":"Local Database", "is_active":1, "type":"sql_db", "config":{"dsn":"foo","username":"user","password":"pass"}}]}';

        $rs = $this->callWithPayload( Verbs::POST, $this->prefix . "/system/service", $payload );
        $data = json_decode( $rs->getContent(), true );
        $this->deleteDbService(9);
        $this->assertTrue( $data['id'] > 0 );
    }

    public function testPOSTServiceWithFields()
    {
        $payload = '{"record":[{"name":"db9","label":"Database","description":"Local Database", "is_active":1, "type":"sql_db", "config":{"dsn":"foo","username":"user","password":"pass"}}]}';

        $rs = $this->callWithPayload( Verbs::POST, $this->prefix . "/system/service?fields=name,label,is_active", $payload );
        $this->deleteDbService(9);
        $this->assertTrue( $rs->getContent() == '{"name":"db9","label":"Database","is_active":1}' );
    }

    public function testPOSTServiceMultiple()
    {
        $payload = '{
            "record":[
                {"name":"db9","label":"Database","description":"Local Database", "is_active":1, "type":"sql_db"},
                {"name":"db10","label":"MyDB","description":"Remote Database", "is_active":1, "type":"sql_db"}
            ]
        }';

        $rs = $this->callWithPayload( Verbs::POST, $this->prefix . "/system/service", $payload );
        $data = json_decode( $rs->getContent(), true );
        $this->deleteDbService(9);
        $this->deleteDbService(10);
        $this->assertTrue(is_array($data['record']));

        foreach($data['record'] as $r)
        {
            $this->assertTrue($r['id']>0);
        }
    }

    public function testPOSTServiceMultipleWithContinue()
    {
        $id1 = $this->createDbService(1);
        $id1++;
        $payload = '{
            "record":[
                {"name":"db9","label":"Database","description":"Local Database", "is_active":1, "type":"sql_db"},
                {"name":"db1","label":"MyDB","description":"Remote Database", "is_active":1, "type":"sql_db"}
            ]
        }';

        $rs = $this->callWithPayload( Verbs::POST, $this->prefix . "/system/service?continue=true", $payload );
        $this->deleteDbService(9);

        $this->assertContains('{"error":{"context":{"errors":[1],"record":[{"id":'.$id1.'},"SQLSTATE[23000]: ', $rs->getContent());
        $this->assertContains("1062 Duplicate entry 'db1' for key 'service_name_unique'", $rs->getContent());
        $this->assertContains('"code":400', $rs->getContent());
        $this->assertResponseStatus(400);
    }

    public function testPOSTServiceMultipleWithRollback()
    {
        $this->createDbService(1);
        $payload = '{
            "record":[
                {"name":"db9","label":"Database","description":"Local Database", "is_active":1, "type":"sql_db"},
                {"name":"db1","label":"MyDB","description":"Remote Database", "is_active":1, "type":"sql_db"}
            ]
        }';

        $rs = $this->callWithPayload( Verbs::POST, $this->prefix . "/system/service?rollback=1", $payload );
        $this->deleteDbService(9);

        $this->assertContains('{"error":{"context":null,"message":"SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry \'db1\'', $rs->getContent());
        $this->assertContains('"code":"23000', $rs->getContent());
        $this->assertResponseStatus(500);
    }

    public function testPOSTServiceMultipleNoWrap()
    {
        $payload = '
            [
                {"name":"db9","label":"Database","description":"Local Database", "is_active":1, "type":"sql_db"},
                {"name":"db10","label":"MyDB","description":"Remote Database", "is_active":1, "type":"sql_db"}
            ]
        ';

        $rs = $this->callWithPayload( Verbs::POST, $this->prefix . "/system/service", $payload );
        $data = json_decode( $rs->getContent(), true );
        $this->deleteDbService(9);
        $this->deleteDbService(10);
        $this->assertTrue(is_array($data['record']));

        foreach($data['record'] as $r)
        {
            $this->assertTrue($r['id']>0);
        }
    }

    public function testPOSTServiceSingleRecord()
    {
        $payload = '{
                        "name":"db1",
                        "label":"Database",
                        "description":"Local Database",
                        "is_active":1,
                        "type":"sql_db"
                    }';

        $this->callWithPayload( Verbs::POST, $this->prefix . "/system/service", $payload );

        $this->assertResponseStatus( 400 );
    }

    public function testPOSTServiceMissingNotNullField()
    {
        $payload = '[{
                        "label":"Database",
                        "description":"Local Database",
                        "is_active":1,
                        "type":"sql_db"
                    }]';

        $rs = $this->callWithPayload( Verbs::POST, $this->prefix . "/system/service", $payload );

        $this->assertResponseStatus( 500 );
        $this->assertContains( '"code":"HY000"', $rs->getContent() );
    }

    /************************************************
     * Testing PUT
     ************************************************/

    public function testPUTServiceById()
    {
        $this->testPATCHServiceById( Verbs::PUT );
    }

    public function testPUTServiceByIds()
    {
        $this->testPATCHServiceByIds( Verbs::PUT );
    }

    public function testPUTServiceBulk()
    {
        $this->testPATCHServiceBulk( Verbs::PUT );
    }

    /************************************************
     * Testing PATCH
     ************************************************/

    public function testPATCHServiceById( $verb = Verbs::PATCH )
    {
        $id1 = $this->createDbService(1);
        $payload = '{
                        "description":"unit-test-string"
                    }';

        $rs = $this->callWithPayload( $verb, $this->prefix . "/system/service/".$id1, $payload );
        $this->assertContains( '{"id":'.$id1.'}', $rs->getContent() );

        $result = $this->call( Verbs::GET, $this->prefix . "/system/service/".$id1 );
        $resultArray = json_decode( $result->getContent(), true );

        $this->assertEquals( "unit-test-string", $resultArray['description'] );
    }

    public function testPATCHServiceByIds( $verb = Verbs::PATCH )
    {
        $id1 = $this->createDbService(1);
        $id2 = $this->createDbService(2);
        $id3 = $this->createDbService(3);

        $payload = '[{
                        "description":"unit-test-description",
                        "label":"unit-test-label"
                    }]';

        $rs = $this->callWithPayload( $verb, $this->prefix . "/system/service?ids=$id1,$id2,$id3", $payload );
        $this->assertContains( '{"record":[{"id":'.$id1.'},{"id":'.$id2.'},{"id":'.$id3.'}]}', $rs->getContent() );

        $result = $this->call( Verbs::GET, $this->prefix . "/system/service?ids=$id1,$id2,$id3" );
        $ra = json_decode( $result->getContent(), true );
        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );

        $this->assertEquals( "unit-test-description,unit-test-description,unit-test-description", $dColumn );
        $this->assertEquals( "unit-test-label,unit-test-label,unit-test-label", $lColumn );
    }

    public function testPATCHServiceBulk( $verb = Verbs::PATCH )
    {
        $id1 = $this->createDbService(1);
        $id2 = $this->createDbService(2);
        $id3 = $this->createDbService(3);

        $payload = '[{
                        "id":'.$id1.',
                        "description":"unit-test-d1",
                        "label":"unit-test-l1"
                    },
                    {
                        "id":'.$id2.',
                        "description":"unit-test-d2",
                        "label":"unit-test-l2"
                    },
                    {
                        "id":'.$id3.',
                        "description":"unit-test-d3",
                        "label":"unit-test-l3"
                    }]';

        $rs = $this->callWithPayload( $verb, $this->prefix . "/system/service", $payload );
        $this->assertContains( '{"record":[{"id":'.$id1.'},{"id":'.$id2.'},{"id":'.$id3.'}]}', $rs->getContent() );

        $result = $this->call( Verbs::GET, $this->prefix . "/system/service?ids=$id1,$id2,$id3" );
        $ra = json_decode( $result->getContent(), true );
        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );

        $this->assertEquals( "unit-test-d1,unit-test-d2,unit-test-d3", $dColumn );
        $this->assertEquals( "unit-test-l1,unit-test-l2,unit-test-l3", $lColumn );
    }

    public function testPATCHServiceBulkWithFields( $verb = Verbs::PATCH )
    {
        $id1 = $this->createDbService(1);
        $id2 = $this->createDbService(2);
        $id3 = $this->createDbService(3);

        $payload = '[{
                        "id":'.$id1.',
                        "description":"unit-test-d1",
                        "label":"unit-test-l1"
                    },
                    {
                        "id":'.$id2.',
                        "description":"unit-test-d2",
                        "label":"unit-test-l2"
                    },
                    {
                        "id":'.$id3.',
                        "description":"unit-test-d3",
                        "label":"unit-test-l3"
                    }]';


        $rs = $this->callWithPayload( $verb, $this->prefix . "/system/service?fields=label", $payload );
        $this->assertContains( '{"record":[{"label":"unit-test-l1"},{"label":"unit-test-l2"},{"label":"unit-test-l3"}]}', $rs->getContent() );
    }

    public function testPATCHServiceBulkWithContinue( $verb = Verbs::PATCH )
    {
        $id1 = $this->createDbService(1);
        $id2 = $this->createDbService(2);
        $id3 = $this->createDbService(3);

        $payload = '[{
                        "id":'.$id1.',
                        "description":"unit-test-d1",
                        "label":"unit-test-l1"
                    },
                    {
                        "id":'.$id2.',
                        "name":"db1",
                        "description":"unit-test-d2",
                        "label":"unit-test-l2"
                    },
                    {
                        "id":'.$id3.',
                        "description":"unit-test-d3",
                        "label":"unit-test-l3"
                    }]';

        $rs = $this->callWithPayload( $verb, $this->prefix . "/system/service?continue=1", $payload );
        $this->assertContains( '{"error":{"context":{"errors":[1],"record":[{"id":'.$id1.'},', $rs->getContent() );
        $this->assertContains(',{"id":'.$id3.'}]}', $rs->getContent());
        $this->assertResponseStatus(400);

        $result = $this->call( Verbs::GET, $this->prefix . "/system/service?ids=$id1,$id2,$id3" );
        $ra = json_decode( $result->getContent(), true );
        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );

        $this->assertEquals( "unit-test-d1,Local Database2,unit-test-d3", $dColumn );
        $this->assertEquals( "unit-test-l1,Database2,unit-test-l3", $lColumn );
    }

    public function testPATCHServiceBulkWithRollback( $verb = Verbs::PATCH )
    {
        $id1 = $this->createDbService(1);
        $id2 = $this->createDbService(2);
        $id3 = $this->createDbService(3);

        $payload = '[{
                        "id":'.$id1.',
                        "description":"unit-test-d1",
                        "label":"unit-test-l1"
                    },
                    {
                        "id":'.$id2.',
                        "name":"db1",
                        "description":"unit-test-d2",
                        "label":"unit-test-l2"
                    },
                    {
                        "id":'.$id3.',
                        "description":"unit-test-d3",
                        "label":"unit-test-l3"
                    }]';

        $rs = $this->callWithPayload( $verb, $this->prefix . "/system/service?rollback=true", $payload );
        $this->assertContains( '{"error":{"context":null,"message":"Failed to update resource: SQLSTATE[23000]: Integrity constraint violation: 1062 Duplicate entry \'db1\' ', $rs->getContent() );
        $this->assertResponseStatus(500);

        $result = $this->call( Verbs::GET, $this->prefix . "/system/service?ids=$id1,$id2,$id3" );
        $ra = json_decode( $result->getContent(), true );
        $dColumn = implode( ",", array_column( $ra['record'], 'description' ) );
        $lColumn = implode( ",", array_column( $ra['record'], 'label' ) );

        $this->assertEquals( "Local Database1,Local Database2,Local Database3", $dColumn );
        $this->assertEquals( "Database1,Database2,Database3", $lColumn );
    }

    /************************************************
     * Testing DELETE
     ************************************************/

    public function testDELETEServiceById()
    {
        $id1 = $this->createDbService(1);
        $rs = $this->call( Verbs::DELETE, $this->prefix . "/system/service/".$id1 );
        $this->assertEquals( '{"id":'.$id1.'}', $rs->getContent() );

        $this->call( Verbs::GET, $this->prefix . "/system/service/".$id1 );
        $this->assertResponseStatus( 404 );
    }

    public function testDELETEServiceByIds()
    {
        $id1 = $this->createDbService(1);
        $id2 = $this->createDbService(2);
        $id3 = $this->createDbService(3);

        $rs = $this->call( Verbs::DELETE, $this->prefix . "/system/service?ids=$id1,$id3" );
        $this->assertEquals( '{"record":[{"id":'.$id1.'},{"id":'.$id3.'}]}', $rs->getContent() );

        $this->call( Verbs::GET, $this->prefix . "/system/service/".$id1 );
        $this->assertResponseStatus( 404 );

        $rs = $this->call( Verbs::GET, $this->prefix . "/system/service/".$id2 );
        $data = json_decode( $rs->getContent(), true );
        $this->assertEquals( "Database2", $data['label'] );
    }

    public function testDELETEServiceBulk()
    {
        $id1 = $this->createDbService(1);
        $id2 = $this->createDbService(2);
        $id3 = $this->createDbService(3);

        $payload = '[{"id":'.$id2.'},{"id":'.$id3.'}]';

        $rs = $this->callWithPayload( Verbs::DELETE, $this->prefix . "/system/service", $payload );
        $this->assertEquals( '{"record":[{"id":'.$id2.'},{"id":'.$id3.'}]}', $rs->getContent() );

        $rs = $this->call( Verbs::GET, $this->prefix . "/system/service/".$id1 );
        $data = json_decode( $rs->getContent(), true );
        $this->assertEquals( "Database1", $data['label'] );

        $this->call( Verbs::GET, $this->prefix . "/system/service/".$id3 );
        $this->assertResponseStatus( 404 );
    }

    public function testDELETEServiceBulkWithFields()
    {
        $this->createDbService(1);
        $id2 = $this->createDbService(2);
        $id3 = $this->createDbService(3);

        $payload = '[{"id":'.$id2.'},{"id":'.$id3.'}]';

        $rs = $this->callWithPayload( Verbs::DELETE, $this->prefix . "/system/service?fields=name,type", $payload );
        $this->assertEquals( '{"record":[{"name":"db2","type":"sql_db"},{"name":"db3","type":"sql_db"}]}', $rs->getContent() );
    }

    /************************************************
     * Internal functions
     ************************************************/

    protected function deleteDbService($num)
    {
        $serviceName = 'db'.$num;
        $service = \DreamFactory\Rave\Models\Service::whereName($serviceName);
        $service->delete();
        return true;
    }

    protected function createDbService($num){
        $serviceName = 'db'.$num;
        $service = \DreamFactory\Rave\Models\Service::whereName($serviceName)->first();

        if(empty($service))
        {
            $service = \DreamFactory\Rave\Models\Service::create(
                [
                    "name"        => $serviceName,
                    "label"       => "Database".$num,
                    "description" => "Local Database".$num,
                    "is_active"   => 1,
                    "type"        => "sql_db",
                    'config'      => [
                        'dsn'        => 'foo',
                        'username'   => 'user',
                        'password'   => 'password',
                        'db'         => 'mydb',
                        'options'    => 'options',
                        'attributes' => 'attributes'
                    ]
                ]
            );
        }

        return $service->id;
    }
}