<?php

namespace NormCache\Spaces;

use NormCache\Values\CacheSpace;

// Resolves the active cache space: explicit ->space(), else the model's first
// declared space, else the default.
final class CacheSpaceResolver
{
    public function __construct(private readonly CacheSpaceRegistry $registry) {}

    public function resolve(string $modelClass, ?string $explicitSpace): CacheSpace
    {
        if ($explicitSpace !== null) {
            if (!$this->registry->modelAllowedInSpace($modelClass, $explicitSpace)) {
                throw new \InvalidArgumentException(
                    "NormCache: model [{$modelClass}] is not a member of space [{$explicitSpace}]; declare it in \$normCacheSpaces or remove ->space()."
                );
            }

            return $this->registry->space($explicitSpace);
        }

        // [0] is the home space: declared order, or [default] when undeclared.
        return $this->registry->spacesForModel($modelClass)[0];
    }
}
