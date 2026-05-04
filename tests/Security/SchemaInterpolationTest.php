<?php

namespace DreamFactory\Core\SqlDb\Tests\Security;

use PHPUnit\Framework\TestCase;

/**
 * Security: Mysql + Postgres getTableConstraints() must use parameterized
 * bindings, not implode("','", $schema) interpolation.
 *
 * Phase 2 audit found the same `IN ('{$schema}')` pattern that was fixed
 * for SQL Server in df-sqlsrv replicated in MySQL and Postgres drivers.
 * The schema parameter flows from the API caller through the schema
 * resource layer; even if the typical caller is admin, the interpolation
 * pattern is a future-bug magnet — any change that widens the input path
 * becomes SQLi.
 *
 * After the fix, both drivers use ? placeholders + a bindings array
 * passed to connection->select(), matching the fix already applied in
 * df-sqlsrv.
 */
class SchemaInterpolationTest extends TestCase
{
    /**
     * @dataProvider driverProvider
     */
    public function testDriverUsesParameterizedSchema(string $relativePath): void
    {
        $absPath = __DIR__ . '/../../' . $relativePath;
        $this->assertFileExists($absPath);
        $contents = file_get_contents($absPath);

        // Slice getTableConstraints body
        $start = strpos($contents, 'function getTableConstraints');
        $this->assertNotFalse($start);
        $end = strpos($contents, "\n    /**", $start + 10);
        $body = substr($contents, $start, $end === false ? null : ($end - $start));

        // Forbid the implode("','", $schema) interpolation pattern.
        $this->assertDoesNotMatchRegularExpression(
            "/implode\s*\(\s*[\"']'\\\\?,\\\\?'[\"']\s*,\s*\\\$schema\s*\)/",
            $body,
            'getTableConstraints() must not interpolate schema names via implode'
        );
        $this->assertDoesNotMatchRegularExpression(
            "/IN\s*\(\s*'\{\\\$schema\}'\s*\)/",
            $body,
            "getTableConstraints() must not interpolate \$schema into IN clause"
        );
        // Require parameterized form.
        $this->assertMatchesRegularExpression(
            '/\$placeholders\s*=\s*implode\s*\(\s*[\'\"],[\'\"]\s*,\s*array_fill/',
            $body,
            'getTableConstraints() must build a placeholder list and pass bindings'
        );
    }

    public static function driverProvider(): array
    {
        return [
            'MySqlSchema'    => ['src/Database/Schema/MySqlSchema.php'],
            'PostgresSchema' => ['src/Database/Schema/PostgresSchema.php'],
        ];
    }
}
