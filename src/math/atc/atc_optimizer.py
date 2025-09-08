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

    def __init__(self, countries: List[str], hops=3):
        self.countries = countries
        self.n = len(countries)

        # Map country -> index
        self.idx = {c: i for i, c in enumerate(countries)}

        # Build directed edge list E (as index pairs)

        edges = self._get_edges_from_countries(countries)
        self.E: List[Tuple[int, int]] = [(self.idx[u], self.idx[v]) for (u, v) in edges if u in self.idx and v in self.idx and u != v]

        # For quick lookups
        self.edge_index = {e: k for k, e in enumerate(self.E)}
        self.out_neighbors = defaultdict(list) # contains all neighbours of an edge
        for (i, j) in self.E:
            self.out_neighbors[i].append(j)

        self.hops = hops

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
        self.indirect_flows = {}   # (i,j,k,n_hops) -> n_hops is number of hops between j and k
        self.max_indirect_flows = {} # (i,j, n_hops) -> max over j of s(i,j,k)
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

        # inequalities for multi-hop slacks
        for hop in range(2, self.hops + 1):
            for i in range(self.n):
                for j in self.out_neighbors[i]:
                    e_ij = edge_var_index(i,  j)

                    if hop == 2:
                        for k in self.out_neighbors[j]:
                            if k == i or k == j:
                                continue

                            # two-hop path i->j->k exists
                            max_flow_ijk = cp.Variable(nonneg=True)
                            self.indirect_flows[(i, j, k, hop)] = max_flow_ijk
                            # min constraints: s(i,j,k) <= flow(i->j) and s(i,j,k) <= flow(j->k)
                            e_jk = edge_var_index(j, k)

                            constr += [
                                max_flow_ijk <= self.flow[e_ij],          # bound by first hop
                                max_flow_ijk <= self.flow[e_jk],          # bound by second hop
                            ]
                    else:
                        for k in range(self.n):
                            if k == i or k == j:
                                continue

                            indirect_flows = [self.indirect_flows[j, v, k, hop - 1] for v in range(self.n) if
                                              (j, v, k, hop - 1) in self.indirect_flows and v != i]

                            if len(indirect_flows) == 0:
                                continue

                            # create max variable for (j,k,hop-1)
                            max_flow_jk = cp.Variable(nonneg=True)
                            self.max_indirect_flows[(i, j, k, hop-1)] = max_flow_jk

                            constr += [max_flow_jk >= cp.max(cp.hstack(indirect_flows))]

                            # now create s(i,j,k) with min constraints
                            max_flow_ijk = cp.Variable(nonneg=True)
                            self.indirect_flows[(i, j, k, hop)] = max_flow_ijk
                            constr += [
                                max_flow_ijk <= self.flow[e_ij],          # bound by first hop
                                max_flow_ijk <= max_flow_jk,                   # bound by other hops
                            ]

        # Equalities for h2h
        for i in range(self.n):
            for k in range(self.n):
                if i == k:
                    continue

                # h2h capacity is direct + indirect flows
                e_ik = edge_var_index(i, k)

                if e_ik is None: # h2h is not known as well
                    continue

                flow_ik = self.flow[e_ik]

                indirect_flows = 0

                for hop in range(2, self.hops + 1):
                    for j in self.out_neighbors[i]:
                        if (i, j, k, hop) in self.indirect_flows:
                            indirect_flows += cp.sum(self.indirect_flows[(i, j, k, hop)])

                constr.append(self.h2h[i, k] >= flow_ik + indirect_flows)

        indirect_flows = cp.sum(list(self.indirect_flows.values()))
        #direct_flows = cp.sum(self.flow)
        max_flows = cp.sum(list(self.max_indirect_flows.values()))

        obj = cp.Maximize(indirect_flows - max_flows) # minimize slack variables
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
        for (i, j, k, n_hops), s in self.indirect_flows.items():
            if n_hops == 2:
                if (i,k) in self.edge_index:
                    e_ij = self.edge_index[(i, j)]
                    e_jk = self.edge_index[(j, k)]

                    #print(i, j, k, s.value, f[e_ij], f[e_jk])

                    err = max(err, abs(s.value - min(f[e_ij], f[e_jk])))
            else:
                pass

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
        ("NL", "FR"): 0.0,
        ("FR", "NL"): 2774.2,
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