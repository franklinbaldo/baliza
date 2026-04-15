/**
 * Baliza Geo-Utility
 * Translates Browser Coordinates -> City Name -> IBGE Code
 */

export interface GeoLocation {
  latitude: number;
  longitude: number;
}

export interface CityResult {
  city: string;
  state: string;
}

interface IBGEResult {
  id: number;
  nome: string;
  microrregiao: {
    mesorregiao: {
      UF: {
        nome: string;
      }
    }
  }
}

export async function getUserCoordinates(): Promise<GeoLocation> {
  return new Promise((resolve, reject) => {
    if (!navigator.geolocation) {
      reject(new Error("Geolocalização não suportada pelo navegador."));
      return;
    }
    navigator.geolocation.getCurrentPosition(
      (pos) => resolve({ latitude: pos.coords.latitude, longitude: pos.coords.longitude }),
      (err) => reject(err),
      { timeout: 10000 }
    );
  });
}

export async function getCityFromCoords(lat: number, lng: number): Promise<CityResult> {
  const url = `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lng}&zoom=10&addressdetails=1`;
  const res = await fetch(url, {
    headers: { 'User-Agent': 'Baliza-Monitor-Transparency-App' }
  });
  if (!res.ok) throw new Error("Falha ao converter coordenadas em cidade.");
  
  const data = await res.json();
  const addr = data.address;
  const city = addr.city || addr.town || addr.village || addr.municipality || "";
  const state = addr.state || "";
  
  return { city, state };
}

export async function getIBGECode(cityName: string, stateName: string): Promise<string | null> {
  const url = `https://servicodados.ibge.gov.br/api/v1/localidades/municipios?nome=${encodeURIComponent(cityName)}`;
  const res = await fetch(url);
  if (!res.ok) return null;
  
  const results = await res.json();
  if (!Array.isArray(results) || results.length === 0) return null;

  const match = (results as IBGEResult[]).find((m) => 
    m.nome.toLowerCase() === cityName.toLowerCase() &&
    (stateName === "" || m.microrregiao.mesorregiao.UF.nome.toLowerCase() === stateName.toLowerCase())
  );

  return match ? String(match.id) : String(results[0].id);
}
