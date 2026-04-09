use crate::commands::geo::decode::Coordinates;

pub fn haversine(origin: Coordinates, destination: Coordinates) -> f64 {
    const R: f64 = 6372797.560856;

    let lat1 = origin.latitude.to_radians();
    let lat2 = destination.latitude.to_radians();
    let d_lat = lat2 - lat1;
    let d_lon = (destination.longitude - origin.longitude).to_radians();

    let a = (d_lat / 2.0).sin().powi(2) + (d_lon / 2.0).sin().powi(2) * lat1.cos() * lat2.cos();
    let c = 2.0 * a.sqrt().asin();
    R * c
}
