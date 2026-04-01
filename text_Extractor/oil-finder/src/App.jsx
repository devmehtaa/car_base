import React, { useEffect, useMemo, useState } from "react";

export default function App() {
  const [data, setData] = useState([]);
  const [brand, setBrand] = useState("");
  const [year, setYear] = useState("");
  const [model, setModel] = useState("");
  const [engine, setEngine] = useState("");
  const [searchInput, setSearchInput] = useState("");
  const [searched, setSearched] = useState(false);
  const [loading, setLoading] = useState(true);

  // ✅ FETCH FROM BACKEND (SQL API)
  useEffect(() => {
    fetch("http://localhost:5000/api/vehicles")
      .then(res => res.json())
      .then(json => {
        setData(json);
        setLoading(false);
      })
      .catch(err => {
        console.error("Error loading data:", err);
        setLoading(false);
      });
  }, []);

  const handleReset = () => {
    setBrand("");
    setYear("");
    setModel("");
    setEngine("");
    setSearchInput("");
    setSearched(false);
  };

  const brands = useMemo(
    () => [...new Set(data.map(v => v.make))].sort(),
    [data]
  );

  const years = useMemo(() => {
    return [
      ...new Set(
        data
          .filter(v => (brand ? v.make === brand : true))
          .map(v => v.year)
      )
    ].sort((a, b) => b - a);
  }, [brand, data]);

  const models = useMemo(() => {
    return [
      ...new Set(
        data
          .filter(
            v =>
              (brand ? v.make === brand : true) &&
              (year ? String(v.year) === year : true)
          )
          .map(v => v.model)
      )
    ].sort();
  }, [brand, year, data]);

  const engines = useMemo(() => {
    return [
      ...new Set(
        data
          .filter(
            v =>
              (brand ? v.make === brand : true) &&
              (year ? String(v.year) === year : true) &&
              (model ? v.model === model : true)
          )
          .map(v => v.engine)
      )
    ].sort();
  }, [brand, year, model, data]);

  const results = useMemo(() => {
    if (!searched) return [];

    if (searchInput) {
      const words = searchInput.toLowerCase().split(" ").filter(Boolean);
      return data.filter(v => {
        const combined =
          `${v.year} ${v.make} ${v.model} ${v.engine || ""}`.toLowerCase();
        return words.every(word => combined.includes(word));
      });
    }

    return data.filter(v =>
      (brand ? v.make === brand : true) &&
      (year ? String(v.year) === year : true) &&
      (model ? v.model === model : true) &&
      (engine ? v.engine === engine : true)
    );
  }, [searched, brand, year, model, engine, searchInput, data]);

  const sortOils = oils =>
    [...oils].sort((a, b) =>
      a.recommendation_level === "primary" ? -1 : 1
    );

  return (
    <div style={styles.page}>
      <nav style={styles.navbar}>
        <div style={styles.logo}>OilFinder Pro</div>

        <div style={styles.navCenter}>
          <input
            style={styles.topSearch}
            placeholder="Quick Search (e.g. 2017 Jeep Renegade 1.4L)"
            value={searchInput}
            onChange={e => {
              setSearchInput(e.target.value);
              setSearched(false);
            }}
          />
          <button style={styles.navButton} onClick={() => setSearched(true)}>
            Search
          </button>
        </div>
      </nav>

      <section style={styles.heroSection}>
        <h1 style={styles.title}>Find the Perfect Engine Oil</h1>
        <p style={styles.subtitle}>
          Manufacturer-verified oil recommendations.
        </p>
      </section>

      <section style={styles.mainContainer}>
        <div style={styles.filterCard}>
          <div style={styles.dropdownGrid}>
            <select style={styles.select} value={brand} onChange={e => {
              setBrand(e.target.value);
              setYear("");
              setModel("");
              setEngine("");
              setSearched(false);
            }}>
              <option value="">Select Brand</option>
              {brands.map(b => <option key={b}>{b}</option>)}
            </select>

            <select style={styles.select} value={year} disabled={!brand} onChange={e => {
              setYear(e.target.value);
              setModel("");
              setEngine("");
              setSearched(false);
            }}>
              <option value="">Select Year</option>
              {years.map(y => <option key={y}>{y}</option>)}
            </select>

            <select style={styles.select} value={model} disabled={!year} onChange={e => {
              setModel(e.target.value);
              setEngine("");
              setSearched(false);
            }}>
              <option value="">Select Model</option>
              {models.map(m => <option key={m}>{m}</option>)}
            </select>

            <select style={styles.select} value={engine} disabled={!model} onChange={e => {
              setEngine(e.target.value);
              setSearched(false);
            }}>
              <option value="">Select Engine</option>
              {engines.map(e => <option key={e}>{e}</option>)}
            </select>
          </div>

          <div style={styles.buttonRow}>
            <button style={styles.primaryButton} onClick={() => setSearched(true)}>
              Get Recommendation
            </button>
            <button style={styles.resetButton} onClick={handleReset}>
              Reset
            </button>
          </div>
        </div>

        {loading && <div style={styles.noResult}>Loading data...</div>}

        {searched && !loading && results.map((vehicle, idx) => (
          <div key={idx} style={styles.resultCard}>
            <h2 style={styles.resultTitle}>
              {vehicle.displayName} {vehicle.engine}
            </h2>

            <div style={styles.capacityBox}>
              <strong>Oil Capacity:</strong>
              {vehicle.capacity?.with_filter ? (
                <div>
                  • With Filter: {vehicle.capacity.with_filter.quarts} qt
                  ({vehicle.capacity.with_filter.liters} L)
                </div>
              ) : (
                <div>Not Available</div>
              )}
            </div>

            {sortOils(vehicle.oils).map((oil, i) => (
              <div key={i} style={styles.oilCard}>
                <div>
                  <div style={styles.oilType}>{oil.oil_type}</div>
                  <div style={styles.smallText}>
                    • {oil.temperature}
                  </div>
                </div>
                <div style={{
                  ...styles.statusBadge,
                  background:
                    oil.recommendation_level === "primary"
                      ? "#16a34a"
                      : "#f59e0b"
                }}>
                  {oil.recommendation_level}
                </div>
              </div>
            ))}
          </div>
        ))}

        {searched && !loading && results.length === 0 && (
          <div style={styles.noResult}>No results found.</div>
        )}
      </section>

      <footer style={styles.footer}>
        © {new Date().getFullYear()} OilFinder Pro
      </footer>
    </div>
  );
}

const styles = {
  page: {
    minHeight: "100vh",
    background: "linear-gradient(135deg,#0f172a,#1e293b,#334155)",
    fontFamily: "'Inter', sans-serif",
    color: "white",
    display: "flex",
    flexDirection: "column"
  },
  navbar: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    padding: "20px 40px",
    background: "rgba(0,0,0,0.4)"
  },
  logo: { fontWeight: 800, fontSize: "20px" },
  navCenter: { display: "flex", gap: "10px", width: "380px" },
  topSearch: { flex: 1, padding: "10px 14px", borderRadius: "8px", border: "none" },
  navButton: {
    padding: "10px 16px",
    background: "#3b82f6",
    border: "none",
    color: "white",
    borderRadius: "8px",
    fontWeight: 600,
    cursor: "pointer"
  },
  heroSection: { textAlign: "center", padding: "50px 20px 20px" },
  title: { fontSize: "40px", fontWeight: 800 },
  subtitle: { opacity: 0.7, marginTop: "8px" },
  mainContainer: { width: "100%", maxWidth: "900px", margin: "40px auto", padding: "0 20px" },
  filterCard: {
    background: "rgba(255,255,255,0.06)",
    padding: "30px",
    borderRadius: "16px",
    backdropFilter: "blur(10px)"
  },
  dropdownGrid: { display: "grid", gridTemplateColumns: "1fr 1fr", gap: "15px", marginBottom: "20px" },
  select: { padding: "12px", borderRadius: "8px", border: "none", fontSize: "14px" },
  buttonRow: { display: "flex", gap: "15px" },
  primaryButton: {
    flex: 1,
    padding: "14px",
    background: "#2563eb",
    color: "white",
    border: "none",
    borderRadius: "10px",
    fontWeight: 600,
    cursor: "pointer"
  },
  resetButton: {
    flex: 1,
    padding: "14px",
    background: "#334155",
    color: "white",
    border: "none",
    borderRadius: "10px",
    cursor: "pointer"
  },
  resultCard: {
    background: "white",
    color: "#1e293b",
    padding: "25px",
    borderRadius: "16px",
    marginTop: "30px",
    boxShadow: "0 8px 25px rgba(0,0,0,0.2)"
  },
  resultTitle: { fontSize: "20px", fontWeight: 700, marginBottom: "15px" },
  capacityBox: { marginBottom: "15px", fontWeight: 600 },
  oilCard: {
    display: "flex",
    justifyContent: "space-between",
    marginBottom: "12px",
    padding: "12px",
    background: "#f1f5f9",
    borderRadius: "12px"
  },
  oilType: { fontSize: "18px", fontWeight: 700 },
  smallText: { fontSize: "13px", color: "#64748b" },
  statusBadge: {
    padding: "6px 14px",
    borderRadius: "20px",
    color: "white",
    fontSize: "12px",
    fontWeight: 600,
    textTransform: "capitalize"
  },
  noOil: { color: "#64748b" },
  noResult: { marginTop: "30px", opacity: 0.6 },
  footer: { marginTop: "auto", textAlign: "center", padding: "20px", opacity: 0.5 }
};