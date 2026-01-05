from __future__ import annotations

from collections import deque

import numpy as np
from sklearn.linear_model import LinearRegression


class QREstimator:
    """Estimate process/observation noise from a rolling window."""

    def __init__(
        self,
        window_size: int = 60,
        base_dim: int = 5,
        winsor_p: float = 0.035,
        q_floor: float = 1e-6,
        q_init: float = 1e-4,
        r_init: float = 1e-4,
        q_shrink: float = 0.5,
    ) -> None:
        self.window_size = int(window_size)
        self.base_dim = int(base_dim)
        self.winsor_p = float(winsor_p)
        self.q_floor = float(q_floor)
        self.q_init = float(q_init)
        self.r_init = float(r_init)
        self.q_shrink = float(q_shrink)

        self.X_window: deque[np.ndarray] = deque(maxlen=self.window_size)
        self.y_window: deque[float] = deque(maxlen=self.window_size)
        self.beta_history: deque[np.ndarray] = deque(maxlen=self.window_size)
        self.prev_beta: np.ndarray | None = None

    @staticmethod
    def _robust_var(values: np.ndarray, p: float = 0.02) -> float:
        values = np.asarray(values, dtype=float)
        if values.size == 0:
            return 0.0
        if p <= 0:
            return float(np.var(values, ddof=1)) if values.size > 1 else 0.0
        lo, hi = np.quantile(values, [p / 2, 1 - p / 2])
        clipped = np.clip(values, lo, hi)
        return float(np.var(clipped, ddof=1)) if clipped.size > 1 else 0.0

    def update_data(self, X_row: np.ndarray, y: float) -> None:
        """Append one observation row and response."""
        X_row = np.asarray(X_row, dtype=float)
        if X_row.shape != (1, self.base_dim):
            raise ValueError(f"X_row shape {X_row.shape} != (1,{self.base_dim})")
        self.X_window.append(X_row)
        self.y_window.append(float(y))

    def estimate(self) -> tuple[np.ndarray, float]:
        """Return (Q, R) for the current window."""
        if len(self.X_window) < self.window_size:
            return np.eye(self.base_dim) * self.q_init, self.r_init

        X = np.vstack(self.X_window)
        y = np.asarray(self.y_window, dtype=float)

        model = LinearRegression(fit_intercept=False).fit(X, y)
        y_pred = model.predict(X)
        residuals = y - y_pred

        R = self._robust_var(residuals, p=self.winsor_p)
        if not np.isfinite(R) or R <= 0:
            R = self.r_init

        current_beta = model.coef_.reshape(-1)
        if self.prev_beta is not None:
            delta_beta = current_beta - self.prev_beta
            self.beta_history.append(delta_beta)
        self.prev_beta = current_beta

        if len(self.beta_history) < max(6, self.base_dim + 1):
            Q = np.eye(self.base_dim) * self.q_init
        else:
            deltas = np.vstack(self.beta_history)
            Q_emp = np.cov(deltas, rowvar=False)
            Q_emp = 0.5 * (Q_emp + Q_emp.T)
            diag = np.clip(np.diag(Q_emp), self.q_floor, None)
            Q = Q_emp.copy()
            np.fill_diagonal(Q, diag)
            shrink = 0.1
            Q = (1 - shrink) * Q + shrink * np.eye(self.base_dim) * np.mean(diag)

        return Q * self.q_shrink, R
