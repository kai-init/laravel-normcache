<?php

namespace NormCache\Tests\Unit;

use NormCache\Tests\UnitTestCase;
use PHPUnit\Framework\Attributes\DataProvider;

final class WorkflowTestPathsTest extends UnitTestCase
{
    #[DataProvider('workflowTestPaths')]
    public function test_a_test_path_named_by_a_workflow_exists(string $workflow, string $path): void
    {
        $this->assertFileExists(
            self::repositoryRoot() . '/' . $path,
            "{$workflow} runs [{$path}], which does not exist.",
        );
    }

    /** @return iterable<string, array{0: string, 1: string}> */
    public static function workflowTestPaths(): iterable
    {
        foreach (glob(self::repositoryRoot() . '/.github/workflows/*.yml') ?: [] as $workflow) {
            $name = basename($workflow);

            preg_match_all(
                '#(?<![\w./-])tests/[\w./-]*[\w]#',
                (string) file_get_contents($workflow),
                $matches,
            );

            foreach (array_unique($matches[0]) as $path) {
                yield "{$name} {$path}" => [$name, $path];
            }
        }
    }

    private static function repositoryRoot(): string
    {
        return dirname(__DIR__, 2);
    }
}
