import { Link, NavLink, useLocation, useNavigate } from "react-router-dom";
import { useTheme } from "../lib/theme";

const REPO = "https://github.com/OxidantData/Oxidant";

function ThemeToggle() {
  const { theme, toggle } = useTheme();
  return (
    <button
      onClick={toggle}
      aria-label="Toggle theme"
      className="oxidant-btn-ghost h-9 w-9 px-0"
      title={theme === "dark" ? "Switch to light" : "Switch to dark"}
    >
      {theme === "dark" ? "☀" : "☾"}
    </button>
  );
}

export default function Navbar() {
  const link = ({ isActive }: { isActive: boolean }) =>
    `text-sm font-medium transition-colors ${
      isActive ? "text-accent" : "text-muted hover:text-body"
    }`;
  const navigate = useNavigate();
  const location = useLocation();
  // HashRouter owns the URL fragment, so #pricing can't be a real anchor — scroll imperatively.
  const goToPricing = () => {
    const scroll = () =>
      document.getElementById("pricing")?.scrollIntoView({ behavior: "smooth", block: "start" });
    if (location.pathname === "/") scroll();
    else {
      navigate("/");
      setTimeout(scroll, 100);
    }
  };
  return (
    <header className="sticky top-0 z-20 border-b border-hairline bg-bg/80 backdrop-blur">
      <div className="oxidant-container flex h-14 items-center justify-between">
        <Link to="/" className="flex items-center gap-2">
          <img src="oxidant.svg" alt="Oxidant" className="h-7 w-7" />
          <span className="text-base font-semibold tracking-tight">Oxidant</span>
        </Link>
        <nav className="flex items-center gap-5 sm:gap-6">
          <NavLink to="/" end className={link}>
            Overview
          </NavLink>
          <NavLink to="/performance" className={link}>
            Benchmarks
          </NavLink>
          <button onClick={goToPricing} className="text-sm font-medium text-muted transition-colors hover:text-body">
            Pricing
          </button>
          <NavLink to="/architecture" className={link}>
            Architecture
          </NavLink>
          <a
            href={`${REPO}#readme`}
            className="hidden text-sm font-medium text-muted transition-colors hover:text-body sm:inline"
          >
            Docs
          </a>
          <ThemeToggle />
          <a href={REPO} className="oxidant-btn-primary hidden sm:inline-flex">
            GitHub
          </a>
        </nav>
      </div>
    </header>
  );
}
