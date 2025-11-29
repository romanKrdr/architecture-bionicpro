import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

type ReportItem = {
  day: string;
  eventsCount: number;
  avgResponseMs: number;
  p95ResponseMs: number;
  errorsCount: number;
};

type UserReportDto = {
  userId: string;
  from: string;
  to: string;
  items: ReportItem[];
};

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<UserReportDto | null>(null);

  const downloadReport = async () => {
    if (!initialized) {
      setError('Keycloak is not initialized yet');
      return;
    }

    if (!keycloak?.token) {
      setError('Пользователь не авторизован');
      return;
    }

    setError(null);
    setLoading(true);

    const to = new Date();
    const from = new Date();
    from.setDate(to.getDate() - 7);

    const toStr = to.toISOString().slice(0, 10);
    const fromStr = from.toISOString().slice(0, 10);

    try {
      const response = await fetch(
          `${API_URL}/reports?from_date=${encodeURIComponent(fromStr)}&to_date=${encodeURIComponent(toStr)}`,
          {
            headers: {
              Authorization: `Bearer ${keycloak.token}`,
            },
          },
      );

      if (response.status === 401) {
        setError('Сессия истекла или пользователь не авторизован');
        setLoading(false);
        return;
      }

      if (!response.ok) {
        setError(`Ошибка при получении отчёта: ${response.status}`);
        setLoading(false);
        return;
      }

      const data: UserReportDto = await response.json();
      setReport(data);
    } catch (e) {
      console.error(e);
      setError('Сетевая ошибка при запросе отчёта');
    } finally {
      setLoading(false);
    }
  };

  return (
      <div className="min-h-screen flex items-center justify-center bg-gray-100">
        <div className="bg-white shadow-md rounded p-8 max-w-3xl w-full">
          <h1 className="text-2xl font-bold mb-4">Отчёт по работе протеза</h1>

          <p className="mb-4 text-gray-700">
            Нажмите кнопку ниже, чтобы получить отчёт по своему протезу за последние 7 дней.
          </p>

          <button
              onClick={downloadReport}
              disabled={loading}
              className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:bg-gray-400"
          >
            {loading ? 'Генерируем отчёт...' : 'Получить отчёт'}
          </button>

          {error && (
              <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
                {error}
              </div>
          )}

          {report && (
              <div className="mt-6">
                <h2 className="text-xl font-semibold mb-2">
                  Отчёт по пользователю {report.userId} ({report.from} — {report.to})
                </h2>
                {report.items.length === 0 ? (
                    <p className="text-gray-600">За выбранный период данных нет.</p>
                ) : (
                    <table className="min-w-full border mt-2">
                      <thead>
                      <tr className="bg-gray-200">
                        <th className="border px-2 py-1">День</th>
                        <th className="border px-2 py-1">Событий</th>
                        <th className="border px-2 py-1">Сред. реакция, мс</th>
                        <th className="border px-2 py-1">P95, мс</th>
                        <th className="border px-2 py-1">Ошибки</th>
                      </tr>
                      </thead>
                      <tbody>
                      {report.items.map((item) => (
                          <tr key={item.day}>
                            <td className="border px-2 py-1">{item.day}</td>
                            <td className="border px-2 py-1 text-right">{item.eventsCount}</td>
                            <td className="border px-2 py-1 text-right">{item.avgResponseMs.toFixed(1)}</td>
                            <td className="border px-2 py-1 text-right">{item.p95ResponseMs.toFixed(1)}</td>
                            <td className="border px-2 py-1 text-right">{item.errorsCount}</td>
                          </tr>
                      ))}
                      </tbody>
                    </table>
                )}
              </div>
          )}
        </div>
      </div>
  );
};

export default ReportPage;