<?php

use DreamFactory\Core\Enums\Verbs;
use DreamFactory\Core\Enums\DataFormats;
use DreamFactory\Core\SqlDb\Services\SqlDb;
use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\Testing\TestServiceRequest;
use DreamFactory\Core\Enums\ApiOptions;

class SqlDbTest extends \DreamFactory\Core\Database\Testing\DbServiceTestCase
{
    /**
     * @const string
     */
    const SERVICE_NAME = 'db';
    /**
     * @const string
     */
    const TABLE_NAME = 'todo';
    /**
     * @const string
     */
    const TABLE_ID = 'id';

    /**
     * @var SqlDb
     */
    protected $service = null;

    protected static $tableCreated = false;

    protected static function serviceConfig(): array
    {
        return [
            'id'          => 1,
            'name'        => static::SERVICE_NAME,
            'label'       => 'SQL Database',
            'description' => 'SQL database for testing',
            'is_active'   => true,
            'type'        => 'sql_db',
            'config'      => [
                'driver'   => env('SQLDB_DRIVER', 'mysql'),
                'host'     => env('SQLDB_HOST', 'localhost'),
                'port'     => env('SQLDB_PORT', 3306),
                'database' => env('SQLDB_DATABASE', 'df_unit_test'),
                'username' => env('SQLDB_USER'),
                'password' => env('SQLDB_PASSWORD')
            ]
        ];
    }

    public function setUp(): void
    {
        parent::setUp();

        $this->service = new SqlDb(static::serviceConfig());

        // Drop leftover test table from previous runs (only on first test).
        // Use a disposable service instance because handleRequest() with a resource
        // path leaves $this->resource set (RestHandler::setResourceMembers never clears it),
        // which corrupts subsequent root-level requests on the same instance.
        if (!static::$tableCreated) {
            try {
                $cleanup = new SqlDb(static::serviceConfig());
                $request = new TestServiceRequest(Verbs::DELETE);
                $cleanup->handleRequest($request, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME);
                unset($cleanup);
            } catch (\Exception $e) {
                // Table doesn't exist, that's fine
            }
        }
    }

    public function tearDown(): void
    {
        parent::tearDown();
    }

    /**
     * Assert that a service response is an error with the expected HTTP status/message.
     * handleRequest() catches exceptions and converts them to error responses
     * rather than letting them propagate, so we check the response status and content.
     * Note: error.code may differ from HTTP status (e.g. 1000 for batch errors).
     */
    protected function assertErrorResponse($rs, $expectedStatus, $expectedMessage = null)
    {
        $data = $rs->getContent();
        $this->assertArrayHasKey('error', $data, 'Expected error response but got: ' . json_encode($data));
        $this->assertEquals($expectedStatus, $rs->getStatusCode());
        if ($expectedMessage !== null) {
            // Search entire error structure (including batch context) for the message
            $errorJson = html_entity_decode(json_encode($data['error']));
            $this->assertStringContainsString($expectedMessage, $errorJson,
                'Expected message "' . $expectedMessage . '" not found in error: ' . $errorJson);
        }
    }

    protected function buildPath($path = '')
    {
        return $this->prefix . '/' . static::SERVICE_NAME . '/' . $path;
    }

    /************************************************
     * Testing GET
     ************************************************/

    public function testDefaultResources()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        // MySQL SqlDb exposes _schema and _table (no _proc/_func — those are PostgreSQL only)
        $this->assertCount(2, $data[static::$wrapper]);
    }

    public function testSchemaEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request, Schema::RESOURCE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertEmpty($data[static::$wrapper]);
    }

    public function testCreateTable()
    {
        // POST to _schema (not _schema/todo) — creating a new table requires the
        // table name in the payload, not in the URL path. POST _schema/todo is for
        // modifying an existing table and throws 404 if the table doesn't exist.
        $payload = '[{
	"name": "' . static::TABLE_NAME . '",
	"field": [
		{
			"name": "id",
			"type": "id"
		},
		{
			"name": "name",
			"type": "string",
			"is_unique": true,
			"allow_null": false
		},
		{
			"name": "complete",
			"type": "boolean",
			"allow_null": true
		}
	]
}]';

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Schema::RESOURCE_NAME);
        $data = $rs->getContent();
        // Response is wrapped: {"resource": [{"name": "todo"}]}
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertSame(static::TABLE_NAME, $data[static::$wrapper][0]['name']);
        static::$tableCreated = true;
    }

    public function testGetRecordsEmpty()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertEmpty($data[static::$wrapper]);
    }

    public function testCreateRecords()
    {
        $payload = '[
            {
                "name": "test1",
                "complete": false
            },
            {
                "name": "test2",
                "complete": true
            },
            {
                "name": "test3"
            }
	    ]';

        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }
        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(3, $data[static::$wrapper]);
    }

    public function testGetRecordById()
    {
        $request = new TestServiceRequest();
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
        $data = $rs->getContent();
        $this->assertTrue($data[static::TABLE_ID] == 1);
    }

    public function testGetRecordsByIds()
    {
        $request = new TestServiceRequest(Verbs::GET, [ApiOptions::IDS => '2,3']);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $ids = implode(",", array_column($data[static::$wrapper], static::TABLE_ID));
        $this->assertTrue($ids == "2,3");
    }

    public function testResourceNotFound()
    {
        $request = new TestServiceRequest(Verbs::GET);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/5');
        $this->assertErrorResponse($rs, 404);
    }

    /************************************************
     * Testing POST
     ************************************************/

    public function testCreateRecord()
    {
        $payload = '[{"name":"test4","complete":false}]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }
        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(1, $data[static::$wrapper]);
    }

    public function testCreateRecordsNoWrap()
    {
        $payload = '[
		{
			"name": "test5",
			"complete": false
		},
		{
			"name": "test6",
			"complete": true
		}
	]';

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(2, $data[static::$wrapper]);
    }

    public function testCreateRecordReturnFields()
    {
        $payload = '[{"name":"test7","complete":true}]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST, [ApiOptions::FIELDS => 'name,complete']);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(1, $data[static::$wrapper]);
        $this->assertArrayHasKey('name', $data[static::$wrapper][0]);
        $this->assertArrayHasKey('complete', $data[static::$wrapper][0]);
    }

    public function testCreateRecordsWithContinue()
    {
        $payload = '[
            {
                "name": "test8",
                "complete": false
            },
            {
                "name": "test5",
                "complete": true
            },
            {
                "name": "test9",
                "complete": null
            }
        ]';

        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }
        $request = new TestServiceRequest(Verbs::POST, [ApiOptions::CONTINUES => true]);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertErrorResponse($rs, 400, 'Batch Error: Not all requested records could be created.');
    }

    public function testCreateRecordsWithRollback()
    {
        $payload = '[
            {
                "name": "testRollback",
                "complete": false
            },
            {
                "name": "test5",
                "complete": true
            },
            {
                "name": "testAfter"
            }
        ]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST, [ApiOptions::ROLLBACK => true]);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertErrorResponse($rs, 400, 'All changes rolled back.');
    }

    public function testCreateRecordBadRequest()
    {
        $payload = '[{"name":"test1", "complete":true}]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        // Duplicate name triggers batch error (HTTP 400) with DB-level duplicate message in context
        $this->assertErrorResponse($rs, 400, 'Duplicate');
    }

    public function testCreateRecordFailNotNullField()
    {
        $payload = '[{"name":null, "complete":true}]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertErrorResponse($rs, 400, "Field 'name' can not be NULL.");
    }

    public function testCreateRecordFailMissingRequiredField()
    {
        $payload = '[{"complete":true}]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertErrorResponse($rs, 400, "Required field 'name' can not be NULL.");
    }

    /************************************************
     * Testing PUT
     ************************************************/

    public function testPUTRecordById()
    {
        $this->testUpdateRecordById(Verbs::PUT);
    }

    public function testPUTRecordByIds()
    {
        $this->testUpdateRecordByIds(Verbs::PUT);
    }

    public function testPUTRecords()
    {
        $this->testUpdateRecords(Verbs::PUT);
    }

    /************************************************
     * Testing PATCH
     ************************************************/

    public function testUpdateRecordById($verb = Verbs::PATCH)
    {
        $payload = '{"name":"test1Update"}';
        $request = new TestServiceRequest($verb);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
        $this->assertEquals(200, $rs->getStatusCode());

        // Verify the record can still be fetched (use fresh service to avoid stale resource state)
        $service2 = new SqlDb(static::serviceConfig());
        $getReq = new TestServiceRequest(Verbs::GET);
        $rs2 = $service2->handleRequest($getReq, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
        $this->assertEquals(200, $rs2->getStatusCode());
    }

    public function testUpdateRecordByIds($verb = Verbs::PATCH)
    {
        // IDS-based updates require the payload as an array of records (even if just one),
        // because unwrapResources() only returns records from array-like payloads.
        $payload = '[{"complete":true}]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }
        $request = new TestServiceRequest($verb, [ApiOptions::IDS => '2,3']);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertEquals(200, $rs->getStatusCode());
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(2, $data[static::$wrapper]);
    }

    public function testUpdateRecords($verb = Verbs::PATCH)
    {
//        $this->assertContains( '{"record":[{"id":1},{"id":2},{"id":3}]}', $rs->getContent() );
//        $dColumn = implode( ",", array_column( $ra[static::$wrapper], 'description' ) );
//        $lColumn = implode( ",", array_column( $ra[static::$wrapper], 'label' ) );
//        $this->assertEquals( "unit-test-d1,unit-test-d2,unit-test-d3", $dColumn );
//        $this->assertEquals( "unit-test-l1,unit-test-l2,unit-test-l3", $lColumn );
        $payload = '[
            {
                "id": 1,
                "name": "test1Update",
                "complete": false
            },
            {
                "id": 2,
                "name": "test2Update",
                "complete": true
            },
            {
                "id": 3,
                "name": "test3Update"
            }
        ]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }

        $request = new TestServiceRequest($verb);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(3, $data[static::$wrapper]);
    }

    public function testUpdateRecordsWithFields($verb = Verbs::PATCH)
    {
        $payload = '[{"id": 4, "name":"test4Update","complete":true}]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }

        $request = new TestServiceRequest($verb, [ApiOptions::FIELDS => 'name,complete']);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey(static::$wrapper, $data);
        $this->assertCount(1, $data[static::$wrapper]);
        $this->assertArrayHasKey('name', $data[static::$wrapper][0]);
        $this->assertArrayHasKey('complete', $data[static::$wrapper][0]);
//        $this->assertContains( '{"record":[{"label":"unit-test-l1"},{"label":"unit-test-l2"},{"label":"unit-test-l3"}]}', $rs->getContent() );
    }

    public function testUpdateRecordsWithContinue($verb = Verbs::PATCH)
    {
        $payload = '[
            {
                "id": 8,
                "name": "test8",
                "complete": false
            },
            {
                "id": 5,
                "name": "test5",
                "complete": true
            },
            {
                "id": 9,
                "name": "test9",
                "complete": null
            }
        ]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }

        $request = new TestServiceRequest($verb, [ApiOptions::CONTINUES => true]);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertErrorResponse($rs, 400, 'Batch Error: Not all requested records could be updated.');
    }

    public function testUpdateRecordsWithRollback($verb = Verbs::PATCH)
    {
        $payload = '[
            {
                "id": 4,
                "name": "testRollback",
                "complete": false
            },
            {
                "id": 19,
                "name": "test5",
                "complete": true
            },
            {
                "id": 6,
                "name": "testAfter"
            }
        ]';
        if (static::$wrapper) {
            $payload = '{"' . static::$wrapper . '":' . $payload . '}';
        }

        $request = new TestServiceRequest($verb, [ApiOptions::ROLLBACK => true]);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertErrorResponse($rs, 400, 'All changes rolled back.');
    }

    /************************************************
     * Testing DELETE
     ************************************************/

    public function testDeleteRecordById()
    {
        $request = new TestServiceRequest(Verbs::DELETE);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
        $this->assertEquals(200, $rs->getStatusCode());

        // Verify record is gone (use fresh service to avoid stale resource state)
        $service2 = new SqlDb(static::serviceConfig());
        $getReq = new TestServiceRequest(Verbs::GET);
        $rs2 = $service2->handleRequest($getReq, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/1');
        $this->assertErrorResponse($rs2, 404);
    }

    public function testDeleteRecordByIds()
    {
        $request = new TestServiceRequest(Verbs::DELETE, [ApiOptions::IDS => '2,3']);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertEquals(200, $rs->getStatusCode());

        // Verify records are gone
        $service2 = new SqlDb(static::serviceConfig());
        $getReq = new TestServiceRequest(Verbs::GET);
        $rs2 = $service2->handleRequest($getReq, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/2');
        $this->assertErrorResponse($rs2, 404);

        $service3 = new SqlDb(static::serviceConfig());
        $rs3 = $service3->handleRequest($getReq, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/3');
        $this->assertErrorResponse($rs3, 404);
    }

    public function testDeleteRecords()
    {
        $payload = '[{"id":4},{"id":5}]';
        $request = new TestServiceRequest(Verbs::DELETE);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertEquals(200, $rs->getStatusCode());

        // Verify records are gone
        $service2 = new SqlDb(static::serviceConfig());
        $getReq = new TestServiceRequest(Verbs::GET);
        $rs2 = $service2->handleRequest($getReq, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/4');
        $this->assertErrorResponse($rs2, 404);

        $service3 = new SqlDb(static::serviceConfig());
        $rs3 = $service3->handleRequest($getReq, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/5');
        $this->assertErrorResponse($rs3, 404);
    }

    public function testDeleteRecordsWithFields()
    {
        $payload = '[{"id":6},{"id":7}]';
        $request = new TestServiceRequest(Verbs::DELETE, [ApiOptions::FIELDS => 'name']);
        $request->setContent($payload, DataFormats::JSON);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertEquals(200, $rs->getStatusCode());

        // Verify records are gone
        $service2 = new SqlDb(static::serviceConfig());
        $getReq = new TestServiceRequest(Verbs::GET);
        $rs2 = $service2->handleRequest($getReq, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/6');
        $this->assertErrorResponse($rs2, 404);

        $service3 = new SqlDb(static::serviceConfig());
        $rs3 = $service3->handleRequest($getReq, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/7');
        $this->assertErrorResponse($rs3, 404);
    }

    public function testDropTable()
    {
        $request = new TestServiceRequest(Verbs::DELETE);
        $rs = $this->service->handleRequest($request, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertEquals(200, $rs->getStatusCode());

        // Verify table is gone
        $service2 = new SqlDb(static::serviceConfig());
        $getReq = new TestServiceRequest(Verbs::GET);
        $rs2 = $service2->handleRequest($getReq, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertErrorResponse($rs2, 404);
    }
}
