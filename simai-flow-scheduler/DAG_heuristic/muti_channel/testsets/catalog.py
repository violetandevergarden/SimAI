"""Category dispatcher for multi-channel benchmarks."""

from DAG_heuristic.muti_channel.testsets import adversarial, random, real


def cases(category: str, *, samples: int = 10, seed: int = 260819):
    if category == "random":
        return random.cases(samples, seed)
    if category == "adversarial":
        return adversarial.cases()
    if category == "real":
        return real.cases()
    if category == "all":
        return [*random.cases(samples, seed), *adversarial.cases(), *real.cases()]
    raise ValueError(f"unknown test-set category: {category}")
