import React from "react";
import "./styles.css";
import { Link, useNavigate } from "react-router-dom";

function Navbar() {
  const navigate = useNavigate();

  function handleLogout() {
    localStorage.removeItem("user");
    navigate("/login");
  }
  return (
    <nav className="navbar navbar-expand-lg">
      <a href="/" className="navbar-brand">
        <img
          src="https://www.freepnglogos.com/uploads/spotify-logo-png/spotify-icon-pink-logo-33.png"
          alt="logo"
          className="logo"
        />
        <span className="brand-name">Spotified</span>
      </a>

      <div className="wrapper">
        <ul className="nav navbar-nav mr-auto">
          <li>
            <Link to="/ourPicks">OurPicks</Link>
          </li>
          <li>
            <Link to="/dashboard">Dashboard</Link>
          </li>
          <li>
            <Link to="/signUp">Sign Up</Link>
          </li>
          {!localStorage.getItem("user") ? (
            <li>
              <Link to="/login">Login</Link>
            </li>
          ) : (
            <li>
              <button onClick={handleLogout}>Logout</button>
            </li>
          )}
        </ul>

        {/* <ul className="nav navbar-nav ml-auto">
          <li className="right"><Link to="/logout">Logout</Link></li>
        </ul> */}
      </div>
    </nav>
  );
}

export default Navbar;
