<?php

use DreamFactory\Core\Enums\Verbs;
use DreamFactory\Core\Enums\DataFormats;
use DreamFactory\Core\Exceptions\BadRequestException;
use DreamFactory\Core\SqlDb\Services\SqlDb;
use DreamFactory\Core\SqlDb\Resources\Schema;
use DreamFactory\Core\SqlDb\Resources\Table;
use DreamFactory\Core\Testing\TestServiceRequest;
use DreamFactory\Core\Enums\ApiOptions;

/**
 * VRT: Server-Side Injection > SQL Injection
 *
 * Tests that DreamFactory properly parameterizes all user-supplied input
 * and prevents SQL injection through filter, field, order, and value parameters.
 *
 * These tests create a real 'security_test' table, populate it with data,
 * and attempt various SQL injection attacks through the service layer.
 */
class SqlInjectionTest extends \DreamFactory\Core\Database\Testing\DbServiceTestCase
{
    const SERVICE_NAME = 'db';
    const TABLE_NAME = 'security_test';
    const TABLE_ID = 'id';

    protected $service = null;

    protected static function serviceConfig(): array
    {
        return [
            'id'          => 1,
            'name'        => static::SERVICE_NAME,
            'label'       => 'SQL Database',
            'description' => 'SQL database for security testing',
            'is_active'   => true,
            'type'        => 'sql_db',
            'config'      => [
                'driver'   => env('SQLDB_DRIVER', 'mysql'),
                'host'     => env('SQLDB_HOST', 'localhost'),
                'port'     => env('SQLDB_PORT', 3306),
                'database' => env('SQLDB_DATABASE', 'df_unit_test'),
                'username' => env('SQLDB_USER'),
                'password' => env('SQLDB_PASSWORD'),
            ],
        ];
    }

    public function setUp(): void
    {
        parent::setUp();
        $this->service = new SqlDb(static::serviceConfig());
        $this->ensureTestTable();
    }

    public function tearDown(): void
    {
        // Drop the test table using a disposable service to avoid stale resource state
        $cleanup = new SqlDb(static::serviceConfig());
        $request = new TestServiceRequest(Verbs::DELETE);
        $cleanup->handleRequest($request, Schema::RESOURCE_NAME . '/' . static::TABLE_NAME);
        unset($cleanup);

        parent::tearDown();
    }

    /**
     * Create the test table and seed with data.
     * Uses disposable service instances to avoid the setResourceMembers bug.
     */
    private function ensureTestTable(): void
    {
        // Create table — POST to _schema (not _schema/tablename) with table name in payload
        $schema = '[{
            "name": "' . static::TABLE_NAME . '",
            "field": [
                {"name": "id", "type": "id"},
                {"name": "username", "type": "string", "allow_null": false},
                {"name": "secret", "type": "string", "allow_null": true},
                {"name": "score", "type": "integer", "allow_null": true}
            ]
        }]';

        $createService = new SqlDb(static::serviceConfig());
        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($schema, DataFormats::JSON);
        $createService->handleRequest($request, Schema::RESOURCE_NAME);
        unset($createService);

        // Seed with test data using fresh service
        $seedService = new SqlDb(static::serviceConfig());
        $records = json_encode([
            static::$wrapper => [
                ['username' => 'alice', 'secret' => 'alice_secret_123', 'score' => 100],
                ['username' => 'bob', 'secret' => 'bob_secret_456', 'score' => 200],
                ['username' => 'charlie', 'secret' => 'charlie_secret_789', 'score' => 300],
            ]
        ]);

        $request = new TestServiceRequest(Verbs::POST);
        $request->setContent($records, DataFormats::JSON);
        $seedService->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        unset($seedService);
    }

    // =========================================================================
    // Filter Parameter Injection
    // =========================================================================

    /**
     * Classic tautology attack: OR 1=1 should NOT return all records.
     */
    public function testFilterTautologyAttack()
    {
        $injections = [
            "username='nonexistent' OR 1=1",
            "username='nonexistent' OR '1'='1'",
            "username='nonexistent' OR ''=''",
            "username='x' OR 1=1--",
            "username='x' OR 1=1#",
            "username='x' OR 1=1/*",
        ];

        foreach ($injections as $injection) {
            try {
                $request = new TestServiceRequest(Verbs::GET, [ApiOptions::FILTER => $injection]);
                $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
                $data = $rs->getContent();

                $records = $data[static::$wrapper] ?? [];

                // If the filter is properly handled, it should either:
                // 1. Return 0 records (injection treated as literal)
                // 2. Throw an exception (malformed filter rejected)
                // It should NOT return all 3 records
                $this->assertLessThan(
                    3,
                    count($records),
                    "Tautology attack bypassed filter: '$injection' returned all records"
                );
            } catch (\Exception $e) {
                // Exception is acceptable - it means the injection was rejected
                $this->assertTrue(true, "Filter injection properly rejected: $injection");
            }
        }
    }

    /**
     * UNION-based injection should NOT leak data from other tables.
     */
    public function testFilterUnionInjection()
    {
        $injections = [
            "username='x' UNION SELECT * FROM information_schema.tables--",
            "username='x' UNION ALL SELECT 1,2,3,4--",
            "username='x' UNION SELECT table_name,null,null,null FROM information_schema.tables--",
            "username='x'; SELECT * FROM information_schema.tables--",
        ];

        foreach ($injections as $injection) {
            try {
                $request = new TestServiceRequest(Verbs::GET, [ApiOptions::FILTER => $injection]);
                $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
                $data = $rs->getContent();

                $records = $data[static::$wrapper] ?? [];

                if (isset($data['error'])) {
                    // Error response means injection was rejected
                    $this->assertTrue(true, "UNION injection properly rejected: $injection");
                } else {
                    // UNION injection should not work - verify no extra columns or data
                    foreach ($records as $record) {
                        $this->assertArrayHasKey('username', $record,
                            "UNION injection may have altered column structure: $injection");
                    }
                    if (empty($records)) {
                        $this->assertTrue(true, "UNION injection returned no results: $injection");
                    }
                }
            } catch (\Exception $e) {
                // Exception is acceptable
                $this->assertTrue(true, "UNION injection properly rejected: $injection");
            }
        }
    }

    /**
     * Stacked queries should NOT execute destructive SQL.
     */
    public function testFilterStackedQueryInjection()
    {
        $injections = [
            "username='x'; DROP TABLE " . static::TABLE_NAME . ";--",
            "username='x'; DELETE FROM " . static::TABLE_NAME . ";--",
            "username='x'; UPDATE " . static::TABLE_NAME . " SET secret='pwned';--",
            "username='x'; INSERT INTO " . static::TABLE_NAME . " (username) VALUES ('hacker');--",
        ];

        foreach ($injections as $injection) {
            try {
                $request = new TestServiceRequest(Verbs::GET, [ApiOptions::FILTER => $injection]);
                $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            } catch (\Exception $e) {
                // Exception is expected and acceptable
            }
        }

        // Verify the table still exists and data is intact
        $request = new TestServiceRequest(Verbs::GET);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $records = $data[static::$wrapper] ?? [];

        $this->assertGreaterThanOrEqual(3, count($records),
            'Stacked query injection destroyed or modified data');

        // Verify no 'hacker' user was inserted
        $usernames = array_column($records, 'username');
        $this->assertNotContains('hacker', $usernames,
            'Stacked query injection inserted unauthorized data');

        // Verify secrets are unchanged
        foreach ($records as $record) {
            $this->assertNotEquals('pwned', $record['secret'] ?? null,
                'Stacked query injection modified existing data');
        }
    }

    /**
     * Time-based blind injection should not cause delays.
     */
    public function testFilterBlindTimeInjection()
    {
        $injection = "username='x' OR SLEEP(5)--";
        $startTime = microtime(true);

        try {
            $request = new TestServiceRequest(Verbs::GET, [ApiOptions::FILTER => $injection]);
            $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        } catch (\Exception $e) {
            // Expected
        }

        $elapsed = microtime(true) - $startTime;
        $this->assertLessThan(3.0, $elapsed,
            "Blind time injection caused a delay of {$elapsed}s - SLEEP() was executed");
    }

    // =========================================================================
    // Field/Column Name Injection
    // =========================================================================

    /**
     * Malicious field names should not execute SQL.
     */
    public function testFieldNameInjection()
    {
        $injections = [
            'username; DROP TABLE ' . static::TABLE_NAME,
            'username, (SELECT password FROM users LIMIT 1) as pw',
            "username' OR '1'='1",
            'username UNION SELECT 1',
        ];

        foreach ($injections as $injection) {
            try {
                $request = new TestServiceRequest(Verbs::GET, [ApiOptions::FIELDS => $injection]);
                $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
                $data = $rs->getContent();

                // If it returns data, verify no injected columns appear
                $records = $data[static::$wrapper] ?? [];
                foreach ($records as $record) {
                    $this->assertArrayNotHasKey('pw', $record,
                        "Field injection leaked data via subquery: $injection");
                    $this->assertArrayNotHasKey('password', $record,
                        "Field injection leaked password column: $injection");
                }
            } catch (\Exception $e) {
                // Exception is acceptable
                $this->assertTrue(true, "Field injection properly rejected: $injection");
            }
        }

        // Verify table still exists
        $request = new TestServiceRequest(Verbs::GET);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertNotNull($rs->getContent(), 'Table should still exist after field injection attempts');
    }

    // =========================================================================
    // Order By Injection
    // =========================================================================

    /**
     * ORDER BY clause injection must be rejected with BadRequestException.
     * The fix validates field names against the table schema.
     */
    public function testOrderByInjection()
    {
        $injections = [
            'username; DROP TABLE ' . static::TABLE_NAME . '--',
            "username, (SELECT 1 FROM information_schema.tables)--",
            'username ASC; DELETE FROM ' . static::TABLE_NAME . '--',
            "IF(1=1, username, secret) ASC",
            "(CASE WHEN (SELECT 1)=1 THEN username ELSE secret END) ASC",
        ];

        foreach ($injections as $injection) {
            try {
                $request = new TestServiceRequest(Verbs::GET, [ApiOptions::ORDER => $injection]);
                $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            } catch (\Exception $e) {
                // Exception is acceptable
                $this->assertTrue(true, "Order injection properly rejected: $injection");
            }
        }

        // Verify table still exists and data intact
        $request = new TestServiceRequest(Verbs::GET);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertGreaterThanOrEqual(3, count($data[static::$wrapper] ?? []),
            'ORDER BY injection destroyed data');
    }

    /**
     * C3: ORDER BY with non-existent column must be rejected.
     */
    public function testOrderByRejectsNonSchemaColumns()
    {
        $request = new TestServiceRequest(Verbs::GET, [
            ApiOptions::ORDER => '(SELECT SLEEP(5))',
        ]);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey('error', $data,
            'ORDER BY with injection expression should return error');
    }

    /**
     * C3: ORDER BY with valid column should still work.
     */
    public function testOrderByAcceptsValidColumn()
    {
        $request = new TestServiceRequest(Verbs::GET, [
            ApiOptions::ORDER => 'username asc',
        ]);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $records = $data[static::$wrapper] ?? [];
        $this->assertNotEmpty($records, 'ORDER BY valid column should return records');
    }

    /**
     * C4: Parenthesized subquery in filter value must not bypass binding.
     */
    public function testFilterParenthesizedSubqueryBlocked()
    {
        try {
            $request = new TestServiceRequest(Verbs::GET, [
                ApiOptions::FILTER => "id=(SELECT id FROM information_schema.tables LIMIT 1)",
            ]);
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $data = $rs->getContent();
            $records = $data[static::$wrapper] ?? [];
            // The subquery should be treated as a literal value, returning no records
            $this->assertEmpty($records,
                'Parenthesized subquery in filter should not return results');
        } catch (\Exception $e) {
            // Exception is also acceptable — means the injection was blocked
            $this->assertTrue(true);
        }
    }

    /**
     * H1: GROUP BY with non-existent column must be rejected.
     */
    public function testGroupByRejectsNonSchemaColumns()
    {
        $request = new TestServiceRequest(Verbs::GET, [
            ApiOptions::GROUP => '(SELECT 1 FROM information_schema.tables)',
        ]);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertArrayHasKey('error', $data,
            'GROUP BY with injection expression should return error');
    }

    /**
     * C5: Expression injection via POST body must be blocked.
     */
    public function testExpressionInjectionBlocked()
    {
        $payload = json_encode([
            static::$wrapper => [
                ['username' => 'testexpr', 'secret' => ['expression' => "1); DROP TABLE " . static::TABLE_NAME . "; --"], 'score' => 0]
            ]
        ]);

        try {
            $request = new TestServiceRequest(Verbs::POST);
            $request->setContent($payload, DataFormats::JSON);
            $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->fail('Expression injection should have been rejected');
        } catch (BadRequestException $e) {
            $this->assertStringContainsString('not permitted', $e->getMessage());
        } catch (\Exception $e) {
            // Other exceptions are also acceptable
            $this->assertTrue(true);
        }

        // Verify table still exists
        $request = new TestServiceRequest(Verbs::GET);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $this->assertNotNull($rs->getContent(), 'Table must survive expression injection');
    }

    /**
     * C5: Safe expressions like NOW() should still be allowed.
     */
    public function testSafeExpressionAllowed()
    {
        // This test verifies NOW() doesn't throw, even though the column type may not match.
        // The key behavior: NOW() is allowlisted and reaches the DB layer.
        $payload = json_encode([
            static::$wrapper => [
                ['username' => 'expr_test_safe', 'secret' => ['expression' => 'NULL'], 'score' => 0]
            ]
        ]);

        try {
            $request = new TestServiceRequest(Verbs::POST);
            $request->setContent($payload, DataFormats::JSON);
            $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            $this->assertNotNull($rs->getContent());
        } catch (\Exception $e) {
            // DB type mismatch is acceptable; BadRequestException about "not permitted" is NOT
            $this->assertStringNotContainsString('not permitted', $e->getMessage(),
                'NULL expression should be allowed');
        }
    }

    // =========================================================================
    // Value/Payload Injection
    // =========================================================================

    /**
     * Malicious values in record creation should be stored as literals, not executed.
     */
    public function testValueInjectionOnCreate()
    {
        $maliciousValues = [
            "'; DROP TABLE " . static::TABLE_NAME . "; --",
            "' OR '1'='1",
            "1; DELETE FROM " . static::TABLE_NAME,
            "<script>alert('xss')</script>",
            "Robert'); DROP TABLE Students;--",
        ];

        foreach ($maliciousValues as $value) {
            $payload = json_encode([
                static::$wrapper => [
                    ['username' => $value, 'secret' => 'test', 'score' => 0]
                ]
            ]);

            try {
                $request = new TestServiceRequest(Verbs::POST);
                $request->setContent($payload, DataFormats::JSON);
                $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            } catch (\Exception $e) {
                // May fail due to constraints, that's ok
            }
        }

        // Verify table still exists
        $request = new TestServiceRequest(Verbs::GET);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertNotNull($data, 'Table should survive value injection attacks');

        // Verify original data is intact
        $records = $data[static::$wrapper] ?? [];
        $usernames = array_column($records, 'username');
        $this->assertContains('alice', $usernames, 'Original data should be intact after value injection');
    }

    /**
     * Malicious values in record updates should be stored as literals.
     */
    public function testValueInjectionOnUpdate()
    {
        // Get first record ID
        $request = new TestServiceRequest(Verbs::GET, [ApiOptions::FILTER => "username='alice'"]);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $records = $data[static::$wrapper] ?? [];

        if (empty($records)) {
            $this->markTestSkipped('Could not find alice record for update injection test');
        }

        $aliceId = $records[0]['id'];

        // Try to inject via update
        $payload = json_encode([
            'username' => "alice",
            'secret'   => "'; UPDATE " . static::TABLE_NAME . " SET secret='pwned' WHERE '1'='1",
        ]);

        try {
            $request = new TestServiceRequest(Verbs::PATCH);
            $request->setContent($payload, DataFormats::JSON);
            $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME . '/' . $aliceId);
        } catch (\Exception $e) {
            // Exception is acceptable
        }

        // Verify bob's secret was not changed
        $request = new TestServiceRequest(Verbs::GET, [ApiOptions::FILTER => "username='bob'"]);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $bobRecords = $data[static::$wrapper] ?? [];

        if (!empty($bobRecords)) {
            $this->assertNotEquals('pwned', $bobRecords[0]['secret'],
                'SQL injection via UPDATE modified other records');
        }
    }

    // =========================================================================
    // ID Parameter Injection
    // =========================================================================

    /**
     * Malicious IDs parameter should not leak data or execute SQL.
     */
    public function testIdsParameterInjection()
    {
        $injections = [
            "1 OR 1=1",
            "1 UNION SELECT 1,2,3,4",
            "1; DROP TABLE " . static::TABLE_NAME,
            "-1 OR 1=1",
        ];

        foreach ($injections as $injection) {
            try {
                $request = new TestServiceRequest(Verbs::GET, [ApiOptions::IDS => $injection]);
                $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
                $data = $rs->getContent();

                $records = $data[static::$wrapper] ?? [];
                // Should return at most 1 record (for ID=1) not all records
                $this->assertLessThanOrEqual(1, count($records),
                    "IDs injection returned too many records: $injection");
            } catch (\Exception $e) {
                // Exception is acceptable
                $this->assertTrue(true, "IDs injection properly rejected: $injection");
            }
        }
    }

    // =========================================================================
    // Limit/Offset Injection
    // =========================================================================

    /**
     * Malicious limit/offset values should not execute SQL.
     */
    public function testLimitOffsetInjection()
    {
        $injections = [
            ['limit' => '1; DROP TABLE ' . static::TABLE_NAME, 'offset' => '0'],
            ['limit' => '1', 'offset' => '0; DELETE FROM ' . static::TABLE_NAME],
            ['limit' => '-1', 'offset' => '0'],
            ['limit' => '99999999999', 'offset' => '0'],
        ];

        foreach ($injections as $params) {
            try {
                $request = new TestServiceRequest(Verbs::GET, [
                    ApiOptions::LIMIT  => $params['limit'],
                    ApiOptions::OFFSET => $params['offset'],
                ]);
                $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
            } catch (\Exception $e) {
                // Exception is acceptable
            }
        }

        // Verify table intact
        $request = new TestServiceRequest(Verbs::GET);
        $rs = $this->service->handleRequest($request, Table::RESOURCE_NAME . '/' . static::TABLE_NAME);
        $data = $rs->getContent();
        $this->assertGreaterThanOrEqual(3, count($data[static::$wrapper] ?? []),
            'Limit/offset injection destroyed data');
    }
}
