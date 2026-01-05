from __future__ import annotations

from typing import Any

import numpy as np


class KalmanFilter:
    """Kalman filter with optional ECM (error correction) augmentation."""

    def __init__(
        self,
        state_dim: int,
        z0: np.ndarray | None = None,
        P0: np.ndarray | None = None,
        *,
        alpha_index: int = -1,
        alpha_rho: float = 0.97,
        use_joseph: bool = True,
        use_ecm: bool = True,
        gamma_rho: float = 0.95,
        q_gamma: float = 1e-4,
    ) -> None:
        """
        Parameters
        ----------
        state_dim : int
            Base state dimension (excluding ECM gamma).
        z0 : np.ndarray | None
            Initial state vector.
        P0 : np.ndarray | None
            Initial covariance matrix.
        alpha_index : int
            Index of alpha term in the base state.
        alpha_rho : float
            AR(1) coefficient for alpha.
        use_joseph : bool
            Use Joseph form covariance update.
        use_ecm : bool
            Enable ECM with gamma state.
        gamma_rho : float
            AR(1) coefficient for gamma.
        q_gamma : float
            Process noise for gamma dimension.
        """
        base_dim = state_dim
        self.use_ecm = bool(use_ecm)
        self.gamma_index: int | None = None

        if self.use_ecm:
            self.gamma_index = base_dim
            self.state_dim = base_dim + 1
        else:
            self.state_dim = base_dim

        self.alpha_index = alpha_index if alpha_index >= 0 else (base_dim - 1)
        self.alpha_rho = float(alpha_rho)
        self.gamma_rho = float(gamma_rho)
        self.use_joseph = bool(use_joseph)

        if z0 is None:
            self.z = np.zeros((self.state_dim, 1))
        else:
            z0_arr = np.asarray(z0, dtype=float)
            if z0_arr.shape[0] == self.state_dim:
                self.z = z0_arr.reshape(self.state_dim, 1)
            elif z0_arr.shape[0] == base_dim and self.use_ecm:
                self.z = np.vstack([z0_arr.reshape(base_dim, 1), [[0.0]]])
            else:
                raise ValueError("z0 shape mismatch w.r.t. use_ecm/state_dim")

        if P0 is None:
            self.P = np.eye(self.state_dim)
        else:
            P0_arr = np.asarray(P0, dtype=float)
            if P0_arr.shape == (self.state_dim, self.state_dim):
                self.P = P0_arr.copy()
            elif P0_arr.shape == (base_dim, base_dim) and self.use_ecm:
                self.P = np.eye(self.state_dim)
                self.P[:base_dim, :base_dim] = P0_arr
                self.P[self.gamma_index, self.gamma_index] = max(q_gamma, 1e-6)
            else:
                raise ValueError("P0 shape mismatch w.r.t. use_ecm/state_dim")

        self.F = np.eye(self.state_dim)
        self.F[self.alpha_index, self.alpha_index] = self.alpha_rho
        if self.use_ecm:
            self.F[self.gamma_index, self.gamma_index] = self.gamma_rho

        self._eps = 1e-12
        self._q_gamma_default = float(q_gamma)

    def _maybe_augment_H_Q(
        self, H: np.ndarray, Q: np.ndarray, te_prev: float
    ) -> tuple[np.ndarray, np.ndarray]:
        H_arr = np.asarray(H, dtype=float).reshape(-1, 1)
        Q_arr = np.asarray(Q, dtype=float)

        base_dim = H_arr.shape[0]
        if self.use_ecm:
            if base_dim + 1 == self.state_dim:
                H_ext = np.zeros((self.state_dim, 1))
                H_ext[:base_dim, 0] = H_arr[:, 0]
                H_ext[self.gamma_index, 0] = float(te_prev)
                H_arr = H_ext
                base_dim = self.state_dim

            if Q_arr.shape == (self.state_dim - 1, self.state_dim - 1):
                Q_ext = np.eye(self.state_dim) * 0.0
                Q_ext[: self.state_dim - 1, : self.state_dim - 1] = Q_arr
                Q_ext[self.gamma_index, self.gamma_index] = self._q_gamma_default
                Q_arr = Q_ext

        if H_arr.shape != (self.state_dim, 1):
            raise ValueError(f"H shape {H_arr.shape} mismatch with state_dim={self.state_dim}")
        if Q_arr.shape != (self.state_dim, self.state_dim):
            raise ValueError(f"Q shape {Q_arr.shape} mismatch with state_dim={self.state_dim}")

        return H_arr, Q_arr

    def step(
        self,
        H: np.ndarray,
        y: float,
        Q: np.ndarray,
        R: float,
        te_prev: float = 0.0,
    ) -> np.ndarray:
        """Run one prediction/update step and return the updated state."""
        H_arr, Q_arr = self._maybe_augment_H_Q(H, Q, te_prev)

        z_pred = self.F @ self.z
        P_pred = self.F @ self.P @ self.F.T + Q_arr

        S = float(H_arr.T @ P_pred @ H_arr) + float(R)
        if S < self._eps:
            S = self._eps
        K = (P_pred @ H_arr) / S

        y_pred = float(H_arr.T @ z_pred)
        residual = float(y) - y_pred

        self.z = z_pred + K * residual

        if self.use_joseph:
            identity = np.eye(self.state_dim)
            KHt = K @ H_arr.T
            self.P = (identity - KHt) @ P_pred @ (identity - KHt).T + K * R * K.T
        else:
            self.P = (np.eye(self.state_dim) - K @ H_arr.T) @ P_pred

        return self.z.copy()

    def set_alpha_rho(self, rho: float) -> None:
        """Update alpha rho in the state transition matrix."""
        self.alpha_rho = float(rho)
        self.F[self.alpha_index, self.alpha_index] = self.alpha_rho

    def set_gamma_rho(self, rho: float) -> None:
        """Update gamma rho in the state transition matrix (if ECM enabled)."""
        if not self.use_ecm:
            return
        self.gamma_rho = float(rho)
        self.F[self.gamma_index, self.gamma_index] = self.gamma_rho

    def current_state(self) -> np.ndarray:
        """Return the current state estimate."""
        return self.z.copy()

    def current_cov(self) -> np.ndarray:
        """Return the current covariance estimate."""
        return self.P.copy()


def _as_float(value: Any) -> float:
    return float(value)
