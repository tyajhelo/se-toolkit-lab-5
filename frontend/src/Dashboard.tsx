import { useEffect, useMemo, useState } from 'react'
import { Bar, Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
)

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelinePoint {
  date: string
  submissions: number
}

interface PassRateRow {
  task: string
  avg_score: number
  attempts: number
}

interface DashboardProps {
  token: string
}

const LAB_OPTIONS = ['lab-01', 'lab-02', 'lab-03', 'lab-04', 'lab-05']

function Dashboard({ token }: DashboardProps) {
  const [selectedLab, setSelectedLab] = useState('lab-04')
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelinePoint[]>([])
  const [passRates, setPassRates] = useState<PassRateRow[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    const controller = new AbortController()

    async function loadDashboardData() {
      try {
        setLoading(true)
        setError('')

        const headers: HeadersInit = {
          Authorization: `Bearer ${token}`,
        }

        const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
          fetch(`/analytics/scores?lab=${selectedLab}`, {
            headers,
            signal: controller.signal,
          }),
          fetch(`/analytics/timeline?lab=${selectedLab}`, {
            headers,
            signal: controller.signal,
          }),
          fetch(`/analytics/pass-rates?lab=${selectedLab}`, {
            headers,
            signal: controller.signal,
          }),
        ])

        if (!scoresRes.ok || !timelineRes.ok || !passRatesRes.ok) {
          throw new Error('Failed to load analytics data')
        }

        const scoresData: ScoreBucket[] = await scoresRes.json()
        const timelineData: TimelinePoint[] = await timelineRes.json()
        const passRatesData: PassRateRow[] = await passRatesRes.json()

        setScores(scoresData)
        setTimeline(timelineData)
        setPassRates(passRatesData)
      } catch (err) {
        if (err instanceof Error && err.name === 'AbortError') {
          return
        }
        setError('Could not load dashboard data')
      } finally {
        setLoading(false)
      }
    }

    void loadDashboardData()

    return () => controller.abort()
  }, [selectedLab, token])

  const scoresChartData = useMemo(
    () => ({
      labels: scores.map((item) => item.bucket),
      datasets: [
        {
          label: 'Scores',
          data: scores.map((item) => item.count),
        },
      ],
    }),
    [scores],
  )

  const timelineChartData = useMemo(
    () => ({
      labels: timeline.map((item) => item.date),
      datasets: [
        {
          label: 'Submissions',
          data: timeline.map((item) => item.submissions),
        },
      ],
    }),
    [timeline],
  )

  return (
    <div>
      <div style={{ marginBottom: '1rem' }}>
        <label htmlFor="lab-select" style={{ marginRight: '0.5rem' }}>
          Select lab:
        </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={(e) => setSelectedLab(e.target.value)}
        >
          {LAB_OPTIONS.map((lab) => (
            <option key={lab} value={lab}>
              {lab}
            </option>
          ))}
        </select>
      </div>

      {loading && <p>Loading dashboard...</p>}
      {error && <p>Error: {error}</p>}

      {!loading && !error && (
        <>
          <section style={{ marginBottom: '2rem' }}>
            <h2>Score distribution</h2>
            <Bar data={scoresChartData} />
          </section>

          <section style={{ marginBottom: '2rem' }}>
            <h2>Submission timeline</h2>
            <Line data={timelineChartData} />
          </section>

          <section>
            <h2>Pass rates</h2>
            <table>
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Average score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {passRates.map((row) => (
                  <tr key={row.task}>
                    <td>{row.task}</td>
                    <td>{row.avg_score}</td>
                    <td>{row.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </>
      )}
    </div>
  )
}

export default Dashboard