import type { IBGEResult } from "./types";

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

// Nominatim and IBGE both return the UF as a full Portuguese name
// ("Rondônia", "São Paulo"). The rest of Baliza stores UF as the two-letter
// sigla, so everything that calls into either API passes the response
// through this table before writing it anywhere else.
const UF_NAME_TO_SIGLA: Record<string, string> = {
  acre: 'AC', alagoas: 'AL', amapá: 'AP', amazonas: 'AM', bahia: 'BA',
  ceará: 'CE', 'distrito federal': 'DF', 'espírito santo': 'ES',
  goiás: 'GO', maranhão: 'MA', 'mato grosso': 'MT', 'mato grosso do sul': 'MS',
  'minas gerais': 'MG', pará: 'PA', paraíba: 'PB', paraná: 'PR',
  pernambuco: 'PE', piauí: 'PI', 'rio de janeiro': 'RJ',
  'rio grande do norte': 'RN', 'rio grande do sul': 'RS', rondônia: 'RO',
  roraima: 'RR', 'santa catarina': 'SC', 'são paulo': 'SP',
  sergipe: 'SE', tocantins: 'TO',
};

export function ufNomeToSigla(name: string | undefined | null): string {
  if (!name) return '';
  const key = name.trim().toLowerCase();
  const mapped = UF_NAME_TO_SIGLA[key];
  if (mapped) return mapped;
  return name.trim().length === 2 ? name.trim().toUpperCase() : '';
}

import ibgeData from './ibge-data.json';

export interface MunicipalityInfo {
  nome: string;
  uf: string;
  populacao: number;
}

export function getMunicipalityInfo(ibge: string): MunicipalityInfo | null {
  const data = ibgeData as Record<string, MunicipalityInfo>;
  return data[ibge] || null;
}

export async function getIBGECode(cityName: string, stateName: string): Promise<string | null> {
  const url = `https://servicodados.ibge.gov.br/api/v1/localidades/municipios?nome=${encodeURIComponent(cityName)}`;
  const res = await fetch(url);
  if (!res.ok) return null;
  
  const results = await res.json();
  if (!Array.isArray(results) || results.length === 0) return null;

  // Strict match by city + state. Homonymous names (e.g. "São Francisco"
  // exists in a dozen states) make a first-result fallback unsafe — it would
  // silently pin the wrong IBGE on the shared city context. When we can't
  // confirm the state, surface null and let the caller show an error.
  const match = (results as IBGEResult[]).find((m) =>
    m.nome.toLowerCase() === cityName.toLowerCase() &&
    (stateName === "" || m.microrregiao.mesorregiao.UF.nome.toLowerCase() === stateName.toLowerCase())
  );

  return match ? String(match.id) : null;
}
