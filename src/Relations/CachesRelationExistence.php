<?php

namespace NormCache\Relations;

use Closure;
use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
use Illuminate\Database\Eloquent\Relations\MorphTo;
use NormCache\CacheableBuilder;
use NormCache\Traits\Cacheable;

/** @mixin CacheableBuilder */
trait CachesRelationExistence
{
    public function has($relation, $operator = '>=', $count = 1, $boolean = 'and', ?Closure $callback = null): static
    {
        if (!is_string($relation) || str_contains($relation, '.')) {
            $this->addCapturedContextReason('dependency', 'whereHas/has relation semantics could not be fully verified');
        } else {
            $this->captureRelationSemantics($relation);
        }

        if (!($operator === '>=' && $count === 1)
            && !($operator === '<' && $count === 1)) {
            $this->addCapturedContextReason('dependency', 'has() count threshold requires explicit dependencies');
        }

        if ($callback !== null) {
            $constraint = $callback;
            $callback = function (EloquentBuilder $query) use ($constraint) {
                $result = $constraint($query);
                $this->captureConstrainedRelationQuery($query);

                return $result;
            };
        }

        return parent::has($relation, $operator, $count, $boolean, $callback);
    }

    protected function captureRelationSemantics(string $name): void
    {
        try {
            $relation = $this->getRelationWithoutConstraints($name);
            $related = $relation->getRelated();
            $query = $relation->getQuery();

            if ($relation instanceof MorphTo) {
                $this->addCapturedContextReason('dependency', 'polymorphic relation dependency could not be fully inferred');
            }

            if (!in_array(Cacheable::class, class_uses_recursive($related::class), true)) {
                $this->addCapturedContextReason('dependency', 'related model does not provide automatic table invalidation');
            }

            if ($query->toBase()->lock !== null) {
                $this->addCapturedContextReason('safety', 'relation query uses a lock');
            }
        } catch (\Throwable) {
            $this->addCapturedContextReason('dependency', 'relation semantics could not be inspected');
        }
    }

    private function captureConstrainedRelationQuery(EloquentBuilder $query): void
    {
        if ($query instanceof CacheableBuilder) {
            $this->mergeCapturedBuilderState($query);
        }

        if ($query->toBase()->lock !== null) {
            $this->addCapturedContextReason('safety', 'relation query uses a lock');
        }
    }
}
