<?php

namespace NormCache\Relations;

use Illuminate\Database\Eloquent\Relations\HasOne;
use NormCache\CacheableBuilder;

class CacheableHasOne extends HasOne
{
    public function getResults()
    {
        $this->applyOneOfManyDependency();

        return parent::getResults();
    }

    public function get($columns = ['*'])
    {
        $this->applyOneOfManyDependency();

        return parent::get($columns);
    }

    private function applyOneOfManyDependency(): void
    {
        if ($this->isOneOfMany() && $this->query instanceof CacheableBuilder) {
            $this->query->dependsOn([$this->related::class]);
            $this->query->acknowledgeOfManySelfJoin();
        }
    }
}
