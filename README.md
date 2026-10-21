# Heat & Air Alert (built in Ghana)

A free, lightweight web page that tells anyone, anywhere in the world, whether today's heat and air quality (including Harmattan dust) are safe, in plain language, with one line of advice.

**Live site:** https://YOUR-USERNAME.github.io/ghana-heat-air-alert/ *(replace after publishing)*

## The problem
Heat and dust are real health risks in Ghana, especially in the north and during Harmattan (roughly late November to March). Official data is technical and hard to find. Children, older people, people with asthma, farmers and outdoor workers have no simple way to know when conditions are dangerous or what to do.

## What it does
- Search any town or city in the world, or tap "Use my location"
- Quick picks for 34 towns across Ghana, where the project started
- Shows one of three levels: **OK**, **Take care**, **Stay indoors**
- Gives short advice for heat and for air and dust
- Shows current numbers and a 24-hour outlook
- Remembers your last town and shows the last saved update if the connection drops
- One-tap WhatsApp sharing
- Tiny single file, works on slow connections and low-cost phones, no login, no personal data collected

## How levels are decided
The overall level is the worse of the heat level and the air level.

| Level | Heat (feels-like) | Air (US AQI) |
|---|---|---|
| OK | below 32°C | 0 to 100 |
| Take care | 32 to 38°C | 101 to 150 |
| Stay indoors | above 38°C | 151 and above |

Thresholds follow common heat-index and US AQI categories. This is general information, not medical advice.

## Data
Weather, air quality and place search come from [Open-Meteo](https://open-meteo.com) (free, no key needed). Air quality values are model estimates, not ground measurements.

## Run it locally
Open `index.html` in a browser. No build step or dependencies.

## Deploy (GitHub Pages)
1. Push this repo to GitHub.
2. Go to **Settings > Pages**.
3. Under **Build and deployment**, choose **Deploy from a branch**, select `main` and `/ (root)`, then save.
4. Your site appears at `https://YOUR-USERNAME.github.io/REPO-NAME/` after a minute or two.

## Customize
- **Change the Ghana quick picks:** edit the `PLACES` list in `index.html` (name, latitude, longitude).
- **Change alert thresholds or advice:** edit `heatLevel`, `airLevel`, `HEAT` and `AIR`.
- **Feedback button:** set `FEEDBACK_URL` to a form link.
- **Visitor counts:** uncomment the GoatCounter line in the `<head>` and add your code.

## Roadmap
- Twi, Hausa and Dagbani translations of the advice
- Cold-weather and other risk levels for places outside the tropics
- Daily WhatsApp status message

## License
MIT. See `LICENSE`.
