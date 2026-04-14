<?php

namespace DreamFactory\Core\SqlDb\Tests\Security;

use DreamFactory\Core\SqlDb\Database\Schema\MySqlSchema;
use DreamFactory\Core\Database\Schema\RoutineSchema;
use DreamFactory\Core\Database\Schema\ParameterSchema;
use Illuminate\Database\ConnectionInterface;
use PHPUnit\Framework\TestCase;

/**
 * Security: MySqlSchema INOUT parameter quoting
 *
 * Before the fix, user-supplied INOUT parameter values were interpolated directly
 * into SET statements:
 *
 *     $pre .= "SET $pName = $value;";
 *
 * An attacker could pass a value like:
 *
 *     1; DROP TABLE users; --
 *
 * which would produce:
 *
 *     SET @param = 1; DROP TABLE users; --;
 *
 * After the fix, all values are routed through quoteValue() (which calls
 * PDO::quote()), so the output is:
 *
 *     SET @param = '1; DROP TABLE users; --';
 *
 * This test class exercises getProcedureStatement() (a protected method) via
 * reflection.  It uses hand-written stub classes for the PDO and connection
 * objects so no live database is required.
 */
class InoutParamQuotingTest extends TestCase
{
    /**
     * Build a MySqlSchema instance wired to a stub connection whose PDO stub
     * uses the driver-agnostic fallback quoting (addcslashes + str_replace)
     * — the same code path Schema::quoteValue() falls through to when the PDO
     * driver returns false from quote().
     *
     * We deliberately make the PDO stub return the correctly-escaped string
     * (matching what a real MySQL PDO driver would produce) so that the test
     * validates the full contract: value goes in → PDO::quote() is called →
     * properly-quoted SQL comes out.
     *
     * @param array<string,string> $quoteMap  Optional override map [raw => quoted].
     * @param callable|null        $onStatement  Called with the SQL when
     *                                           connection->statement() fires.
     */
    private function buildSchema(array $quoteMap = [], ?callable $onStatement = null): MySqlSchema
    {
        $pdoStub = new class($quoteMap) extends \stdClass {
            private array $map;
            public function __construct(array $map) { $this->map = $map; }
            public function quote(string $value): string
            {
                if (isset($this->map[$value])) {
                    return $this->map[$value];
                }
                // Real PDO MySQL behaviour: wrap in singles, escape interior singles.
                return "'" . str_replace("'", "\\'", $value) . "'";
            }
        };

        $connection = new class($pdoStub, $onStatement) implements ConnectionInterface {
            private object $pdo;
            private $onStatement;

            public function __construct(object $pdo, ?callable $onStatement)
            {
                $this->pdo         = $pdo;
                $this->onStatement = $onStatement;
            }

            /** Called by Schema::quoteValue() */
            public function getPdo(): object { return $this->pdo; }

            /** Called at the end of the INOUT preamble loop */
            public function statement($query, $bindings = []): bool
            {
                if ($this->onStatement !== null) {
                    ($this->onStatement)($query);
                }
                return true;
            }

            // --- Remaining ConnectionInterface stubs (not exercised) ----------
            public function table($table, $as = null) { return null; }
            public function raw($value) { return null; }
            public function selectOne($query, $bindings = [], $useReadPdo = true) { return null; }
            public function scalar($query, $bindings = [], $useReadPdo = true) { return null; }
            public function select($query, $bindings = [], $useReadPdo = true) { return []; }
            public function cursor($query, $bindings = [], $useReadPdo = true) { return new \EmptyIterator(); }
            public function insert($query, $bindings = []) { return false; }
            public function update($query, $bindings = []) { return 0; }
            public function delete($query, $bindings = []) { return 0; }
            public function unprepared($query) { return false; }
            public function affectingStatement($query, $bindings = []) { return 0; }
            public function prepareBindings(array $bindings) { return $bindings; }
            public function transaction(\Closure $callback, $attempts = 1) { return null; }
            public function beginTransaction() {}
            public function commit() {}
            public function rollBack($toLevel = null) {}
            public function transactionLevel() { return 0; }
            public function pretend(\Closure $callback) { return []; }
            public function getDatabaseName() { return 'test'; }
        };

        return new MySqlSchema($connection);
    }

    /**
     * Build a RoutineSchema stub with a fixed quotedName.
     */
    private function buildRoutine(string $name = 'test_proc'): RoutineSchema
    {
        $routine             = new RoutineSchema(['name' => $name]);
        $routine->quotedName = "`{$name}`";
        return $routine;
    }

    /**
     * Build a ParameterSchema stub.
     */
    private function buildParam(string $name, string $paramType): ParameterSchema
    {
        return new ParameterSchema(['name' => $name, 'param_type' => $paramType]);
    }

    /**
     * Call the protected getProcedureStatement() via reflection.
     */
    private function callGetProcedureStatement(
        MySqlSchema $schema,
        RoutineSchema $routine,
        array $paramSchemas,
        array &$values
    ): string {
        $method = new \ReflectionMethod(MySqlSchema::class, 'getProcedureStatement');
        $method->setAccessible(true);
        // invokeArgs() with a reference to $values satisfies the &$values signature.
        $args = [$routine, $paramSchemas, &$values];
        return $method->invokeArgs($schema, $args);
    }

    // =========================================================================
    // String values
    // =========================================================================

    public function testPlainStringValueIsQuoted(): void
    {
        $captured = null;
        $schema   = $this->buildSchema([], function (string $sql) use (&$captured) {
            $captured = $sql;
        });

        $routine = $this->buildRoutine();
        $params  = [$this->buildParam('p1', 'INOUT')];
        $values  = ['hello world'];

        $this->callGetProcedureStatement($schema, $routine, $params, $values);

        $this->assertNotNull($captured, 'connection->statement() was never called');
        $this->assertStringContainsString("SET @p1 = 'hello world';", $captured,
            'Plain string value must be single-quoted in the SET statement');
    }

    public function testStringWithSingleQuoteIsEscaped(): void
    {
        $captured = null;
        $schema   = $this->buildSchema([], function (string $sql) use (&$captured) {
            $captured = $sql;
        });

        $routine = $this->buildRoutine();
        $params  = [$this->buildParam('p1', 'INOUT')];
        $values  = ["it's a trap"];

        $this->callGetProcedureStatement($schema, $routine, $params, $values);

        $this->assertNotNull($captured);

        // The raw unescaped value must NOT appear verbatim — that would break out of the
        // string literal and open an injection vector.
        $this->assertStringNotContainsString("SET @p1 = it's a trap;", $captured,
            'Unescaped single quote must not appear raw in the SET statement');

        // There must still be a SET @p1 statement
        $this->assertStringContainsString('SET @p1 = ', $captured,
            'SET statement must be present');
    }

    // =========================================================================
    // SQL injection payloads
    // =========================================================================

    /**
     * @dataProvider injectionPayloadProvider
     */
    public function testInjectionPayloadIsContainedInQuotes(string $payload): void
    {
        $captured = null;
        $schema   = $this->buildSchema([], function (string $sql) use (&$captured) {
            $captured = $sql;
        });

        $routine = $this->buildRoutine();
        $params  = [$this->buildParam('p1', 'INOUT')];
        $values  = [$payload];

        $this->callGetProcedureStatement($schema, $routine, $params, $values);

        $this->assertNotNull($captured, 'connection->statement() was never called');

        // The SET statement must start correctly.
        $this->assertStringStartsWith('SET @p1 = ', $captured,
            "SET statement must start with 'SET @p1 = ' for payload: {$payload}");

        // The payload must NOT appear verbatim and unquoted immediately after '= '.
        // If it does, there is no quoting and the payload could break out of context.
        $unquotedInterpolation = "SET @p1 = {$payload};";
        $this->assertStringNotContainsString($unquotedInterpolation, $captured,
            "Payload was interpolated without quoting — SQL injection possible: {$payload}");
    }

    public static function injectionPayloadProvider(): array
    {
        return [
            'single quote escape attempt'          => ["'"],
            'single quote with comment'            => ["' --"],
            'classic OR tautology'                 => ["' OR '1'='1"],
            'stacked statement with semicolon'     => ["1; DROP TABLE users; --"],
            'UNION SELECT exfiltration'            => ["1 UNION SELECT password FROM users --"],
            'comment-terminated injection'         => ["1' OR 1=1 --"],
            'semicolon then DELETE'                => ["'; DELETE FROM procedures WHERE '1'='1"],
            'null byte'                            => ["\x00malicious"],
            'UNION with multiple columns'          => ["x' UNION SELECT 1,2,3,4 --"],
            'subquery injection'                   => ["(SELECT password FROM users LIMIT 1)"],
        ];
    }

    // =========================================================================
    // Numeric values
    // =========================================================================

    public function testIntegerValuePassesThrough(): void
    {
        $captured = null;
        $schema   = $this->buildSchema([], function (string $sql) use (&$captured) {
            $captured = $sql;
        });

        $routine = $this->buildRoutine();
        $params  = [$this->buildParam('amount', 'INOUT')];
        $values  = [42];

        $this->callGetProcedureStatement($schema, $routine, $params, $values);

        $this->assertNotNull($captured);
        // quoteValue() returns numeric values as-is — they cannot embed SQL syntax,
        // so unquoted output is safe and expected.
        $this->assertStringContainsString('SET @amount = 42;', $captured,
            'Integer values should be placed unquoted (quoteValue passes them through)');
    }

    public function testFloatValuePassesThrough(): void
    {
        $captured = null;
        $schema   = $this->buildSchema([], function (string $sql) use (&$captured) {
            $captured = $sql;
        });

        $routine = $this->buildRoutine();
        $params  = [$this->buildParam('ratio', 'INOUT')];
        $values  = [3.14];

        $this->callGetProcedureStatement($schema, $routine, $params, $values);

        $this->assertNotNull($captured);
        $this->assertStringContainsString('SET @ratio = 3.14;', $captured,
            'Float values should be placed unquoted (quoteValue passes them through)');
    }

    // =========================================================================
    // NULL values
    // =========================================================================

    public function testNullValueBecomesUnquotedNull(): void
    {
        $captured = null;
        $schema   = $this->buildSchema([], function (string $sql) use (&$captured) {
            $captured = $sql;
        });

        $routine = $this->buildRoutine();
        $params  = [$this->buildParam('p1', 'INOUT')];
        $values  = [null];

        $this->callGetProcedureStatement($schema, $routine, $params, $values);

        $this->assertNotNull($captured, 'connection->statement() was never called for INOUT NULL');
        $this->assertStringContainsString('SET @p1 = NULL;', $captured,
            'NULL value must produce the unquoted SQL keyword NULL');
    }

    // =========================================================================
    // Mixed IN + INOUT: IN params must remain as PDO placeholders
    // =========================================================================

    public function testInParamRemainsAsPlaceholder(): void
    {
        $captured = null;
        $schema   = $this->buildSchema([], function (string $sql) use (&$captured) {
            $captured = $sql;
        });

        $routine = $this->buildRoutine();
        $params  = [
            $this->buildParam('in_val', 'IN'),
            $this->buildParam('inout_val', 'INOUT'),
        ];
        $values  = ['ignored_for_in', "'; DROP TABLE t; --"];

        $callSql = $this->callGetProcedureStatement($schema, $routine, $params, $values);

        // IN param must appear as a PDO named placeholder in the CALL
        $this->assertStringContainsString(':in_val', $callSql,
            'IN parameter must appear as a PDO named placeholder, not interpolated');

        // The INOUT injection payload must NOT appear raw in the CALL statement
        $this->assertStringNotContainsString("'; DROP TABLE t; --", $callSql,
            'INOUT injection payload must not appear raw in the CALL statement');

        // The SET preamble must have been emitted (for the INOUT)
        $this->assertNotNull($captured,
            'connection->statement() must have been called for the INOUT SET preamble');

        // The payload must be safely contained in the SET statement (not raw)
        $this->assertStringNotContainsString("SET @inout_val = '; DROP TABLE t; --", $captured,
            'INOUT injection payload must not appear unquoted in the SET preamble');
    }

    // =========================================================================
    // OUT-only params: no SET statement should be emitted
    // =========================================================================

    public function testOutOnlyParamProducesNoSetStatement(): void
    {
        $statementCalled = false;
        $schema          = $this->buildSchema([], function () use (&$statementCalled) {
            $statementCalled = true;
        });

        $routine = $this->buildRoutine();
        $params  = [$this->buildParam('result', 'OUT')];
        $values  = [];

        $callSql = $this->callGetProcedureStatement($schema, $routine, $params, $values);

        $this->assertFalse($statementCalled,
            'OUT-only parameters must not trigger a SET statement');
        $this->assertStringContainsString('CALL `test_proc`(@result)', $callSql,
            'OUT parameter must appear in the CALL statement using @var syntax');
    }
}
