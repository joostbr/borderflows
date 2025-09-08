import cvxpy as cp
import numpy as np
from collections import defaultdict
from typing import Dict, Iterable, Tuple, List
from enum import Enum

ATC_EDGES = {
    "BE": ("NL", "FR", "DE"),
    "FR": ("BE", "DE"),
    "DE": ("NL", "BE", "FR"),
    "NL": ("BE", "DE"),
}


class ATCGraphOptimizer:
    """
    Graph-based ATC optimizer.

    Nodes  : countries (indices 0..N-1)
    Edges  : allowed directed edges (i->j) and (j -> i). We create a flow var only for these.
    H2H[i,j] is matched by:  flow(i->j) + sum_{k: (i->j) & (j->k) in E} slack[i,j,k]
    with constraints: slack[i,j,k] <= flow(i->j) and slack[i,j,k] <= flow(j->k).

    Edges not in E are *physically impossible* and have no variable (implicitly 0).
    """

    def __init__(self, countries: List[str]):
        self.countries = countries
        self.n = len(countries)

        # Map country -> index
        self.idx = {c: i for i, c in enumerate(countries)}

        # Build directed edge list E (as index pairs)

        edges = self._get_edges_from_countries(countries)
        self.E: List[Tuple[int, int]] = [(self.idx[u], self.idx[v]) for (u, v) in edges if u in self.idx and v in self.idx and u != v]

        # For quick lookups
        self.edge_index = {e: k for k, e in enumerate(self.E)}
        self.out_neighbors = defaultdict(list)
        for (i, j) in self.E:
            self.out_neighbors[i].append(j)

        # Build problem
        self._build_problem()

    def _get_edges_from_countries(self, countries):
        edges = []

        for country in countries:
                for neighbor in ATC_EDGES[country]:
                    if neighbor in countries:
                        edges.append((country, neighbor))

        return edges

    def _build_problem(self):
        m = len(self.E)                           # number of active directed edges
        self.flow = cp.Variable(m, nonneg=True)   # flow on each directed edge in E

        # For each ordered pair (i,j) we will:
        #  - have an H2H parameter h2h[i,j] (0 if unknown)
        #  - build slack variables s_{i,j,k} only if (i->j) and (j->k) are in E.
        self.h2h = cp.Parameter((self.n, self.n), nonneg=True)

        # Collect slack variable blocks and constraints
        self.slack_blocks = {}   # (i,j) -> cp.Variable(len(K_ij), nonneg=True)
        self.K_lists = {}        # (i,j) -> list of k (indices) for which we created slack
        constr = []

        # Equality per (i,j): h2h[i,j] == flow(i->j) + sum_k slack(i,j,k)  if (i->j) in E
        # If (i->j) not in E, h2h[i,j] == sum_k slack(i,j,k) (but there can be no k if (i->j) not in E),
        # so effectively h2h[i,j] must be 0 unless there is an allowable two-hop via some j (but we only
        # permit slack if (i->j) exists to represent "indirect via j" starting with (i->j).)
        #
        # To stay faithful to your original formulation, we *only* form slack if (i->j) in E.
        # That means h2h[i,j] equals the direct flow plus min-coupled slacks via j->k.
        #
        # If you want h2h between nodes without a direct arc (i->j not in E),
        # they’ll be 0 in this model (which matches "physically impossible" direct flow).
        #
        # If you need multi-hop (i->...->j) support without (i->j), extend with additional
        # layers; here we follow your 2-hop (i->j->k) slack pattern.

        # Build equality + min constraints
        # flow(i->j) index if exists
        def edge_var_index(i, j):
            return self.edge_index.get((i, j), None)

        total_slack_terms = []
        for i in range(self.n):
            for j in range(self.n):
                if i == j:
                    # No self loop H2H; force to 0
                    constr.append(self.h2h[i, j] == 0)
                    continue

                e_ij = edge_var_index(i, j)
                # Determine possible middle nodes k such that (j->k) also exists
                K = []
                if e_ij is not None:
                    # Only create slack if the first hop (i->j) exists physically
                    for k in self.out_neighbors[j]:
                        if k != i and k != j:
                            # two-hop path i->j->k exists
                            K.append(k)

                if K:
                    s = cp.Variable(len(K), nonneg=True)
                    self.slack_blocks[(i, j)] = s
                    self.K_lists[(i, j)] = K
                    # min constraints: s(i,j,k) <= flow(i->j) and s(i,j,k) <= flow(j->k)
                    for idx_k, k in enumerate(K):
                        e_jk = edge_var_index(j, k)
                        constr += [
                            s[idx_k] <= self.flow[e_ij],          # bound by first hop
                            s[idx_k] <= self.flow[e_jk],          # bound by second hop
                        ]
                    # h2h equality:
                    constr.append(self.h2h[i, j] == self.flow[e_ij] + cp.sum(s))
                    total_slack_terms.append(cp.sum(s))
                else:
                    # No slack terms for (i,j)
                    if e_ij is not None:
                        constr.append(self.h2h[i, j] == self.flow[e_ij])
                    else:
                        constr.append(self.h2h[i, j] == 0)

        # Objective — same as your original: maximize total slack (can switch to something else)
        obj = cp.Maximize(cp.sum(total_slack_terms) if total_slack_terms else 0)
        self.prob = cp.Problem(obj, constr)

    def _set_h2h(self, hub_capacities: Dict[Tuple[str, str], float]):
        """Fill the H2H parameter matrix. Missing pairs default to 0."""
        mat = np.zeros((self.n, self.n), dtype=float)
        for (u, v), cap in hub_capacities.items():
            if u in self.idx and v in self.idx and u != v:
                mat[self.idx[u], self.idx[v]] = float(cap)
        self.h2h.value = mat

    def solve(self, hub_capacities, **solver_kw) -> Dict[Tuple[str, str], float]:
        self._set_h2h(hub_capacities)

        self.prob.solve(solver=cp.CLARABEL, **solver_kw)
        return self.get_flows()

    def get_flows(self) -> Dict[Tuple[str, str], float]:
        """Return flows for existing edges only (others are implicitly 0)."""
        vals = {}
        if self.flow.value is None:
            return vals
        for k, (i, j) in enumerate(self.E):
            vals[(self.countries[i], self.countries[j])] = float(self.flow.value[k])
        return vals

    def max_approx_error(self) -> float:
        """
        Same diagnostic as your original: check || s(i,j,k) - min(flow(i->j), flow(j->k)) ||_inf
        over all created slack terms.
        """
        if self.flow.value is None:
            return np.inf
        err = 0.0
        f = self.flow.value
        for (i, j), s in self.slack_blocks.items():
            e_ij = self.edge_index[(i, j)]
            for idx_k, k in enumerate(self.K_lists[(i, j)]):
                e_jk = self.edge_index[(j, k)]
                err = max(err, abs(s.value[idx_k] - min(f[e_ij], f[e_jk])))

                #print(i, j, abs(s.value[idx_k] - min(f[e_ij], f[e_jk])))

        return float(err)


if __name__ == "__main__":
    countries = ["NL", "BE", "DE", "FR"]

    atc = ATCGraphOptimizer(countries)

    # H2H capacities (pairs not listed default to 0). These are the *targets* in the equality.
    hub_capacities = {
        ("NL", "BE"): 0.0,       # no direct var; h2h is forced by model = 0
        ("BE", "NL"): 1411.2,
        ("NL", "DE"): 770.8,
        ("DE", "NL"): 2285.6,
        #("NL", "FR"): 0.0,
        #("FR", "NL"): 2774.2,
        ("BE", "DE"): 1411.2,
        ("DE", "BE"): 0.0,
        ("BE", "FR"): 0.0,
        ("FR", "BE"): 1752.0,
        ("DE", "FR"): 0.0,
        ("FR", "DE"): 2738.5,
    }

    flows = atc.solve(hub_capacities)

    print("Optimized flows (only on allowed edges):")
    for k, v in flows.items():
        print(f"{k}: {v:.3f}")

    print("Max approximation error:", atc.max_approx_error())