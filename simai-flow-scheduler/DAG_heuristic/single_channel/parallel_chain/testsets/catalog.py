"""Category dispatcher for parallel-chain benchmarks."""

from DAG_heuristic.single_channel.parallel_chain.testsets import adversarial, random, real


def cases(category: str, *, samples: int = 10, seed: int = 260813):
    if category == "random":
        return random.cases(samples, seed)
    if category == "adversarial":
        return adversarial.cases()
    if category == "real":
        return real.cases()
    if category == "all":
        return [*random.cases(samples, seed), *adversarial.cases(), *real.cases()]
    raise ValueError(f"unknown test-set category: {category}")
