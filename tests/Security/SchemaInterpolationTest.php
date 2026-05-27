<?php

namespace DreamFactory\Core\SqlDb\Tests\Security;

use PHPUnit\Framework\TestCase;

/**
 * Security: schema/routine metadata lookups in Mysql + Postgres drivers must
 * use parameterized bindings instead of interpolating caller-influenced values.
 *
 * Phase 2 audit fixed getTableConstraints(). Follow-up review found the same
 * interpolation pattern still present in:
 *  - routine parameter introspection (loadParameters)
 *  - Postgres schema-scoped table/view discovery
 */
class SchemaInterpolationTest extends TestCase
{
    public function testPostgresSchemaScopedDiscoveryUsesBindings(): void
    {
        $absPath = __DIR__ . '/../../src/Database/Schema/PostgresSchema.php';
        $this->assertFileExists($absPath);
        $contents = file_get_contents($absPath);

        foreach (['function getTableNames', 'function getViewNames'] as $needle) {
            $start = strpos($contents, $needle);
            $this->assertNotFalse($start, $needle . ' must exist');
            $end = strpos($contents, "\n    /**", $start + 10);
            $body = substr($contents, $start, $end === false ? null : ($end - $start));

            $this->assertStringNotContainsString("table_schema = '\$schema'", $body);
            $this->assertTrue(
                str_contains($body, 'table_schema = ?') || str_contains($body, 'table_schema = :schema'),
                $needle . ' must use a bound schema placeholder'
            );
        }
    }

    /**
     * @dataProvider loadParametersProvider
     */
    public function testRoutineParameterLookupUsesBindings(string $relativePath): void
    {
        $absPath = __DIR__ . '/../../' . $relativePath;
        $this->assertFileExists($absPath);
        $contents = file_get_contents($absPath);

        $start = strpos($contents, 'function loadParameters');
        $this->assertNotFalse($start);
        $end = strpos($contents, "\n    protected function", $start + 10);
        $body = substr($contents, $start, $end === false ? null : ($end - $start));

        $this->assertDoesNotMatchRegularExpression(
            "/ROUTINE_NAME = '\{\\\$holder->resourceName\}'/",
            $body,
            'loadParameters() must not interpolate routine names directly into SQL'
        );
        $this->assertDoesNotMatchRegularExpression(
            "/ROUTINE_SCHEMA = '\{\\\$holder->schemaName\}'/",
            $body,
            'loadParameters() must not interpolate schema names directly into SQL'
        );
        $this->assertMatchesRegularExpression(
            '/ROUTINE_NAME = :routineName|ROUTINE_NAME = \?/',
            $body,
            'loadParameters() must use a parameter placeholder for routine name'
        );
        $this->assertMatchesRegularExpression(
            '/ROUTINE_SCHEMA = :schemaName|ROUTINE_SCHEMA = \?/',
            $body,
            'loadParameters() must use a parameter placeholder for schema name'
        );
    }

    public static function loadParametersProvider(): array
    {
        return [
            'MySqlSchema'    => ['src/Database/Schema/MySqlSchema.php'],
            'PostgresSchema' => ['src/Database/Schema/PostgresSchema.php'],
        ];
    }
}
