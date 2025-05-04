WITH categories AS (
    SELECT
        CAST(id AS INTEGER) AS id_category,
        CAST(category_name AS VARCHAR(100)) AS category_name,
        
    FROM (VALUES
        (1, 'Películas y Animación (Film & Animation)'),
        (2, 'Automóviles y Vehículos (Autos & Vehicles)'),
        (10, 'Música (Music)'),
        (15, 'Mascotas y Animales (Pets & Animals)'),
        (17, 'Deportes (Sports)'),
        (18, 'Cortos (Short Movies)'),
        (19, 'Viajes y Eventos (Travel & Events)'),
        (20, 'Juegos (Gaming)'),
        (21, 'Videoblogs (Videoblogging)'),
        (22, 'Gente y Blogs (People & Blogs)'),
        (23, 'Comedia (Comedy)'),
        (24, 'Entretenimiento (Entertainment)'),
        (25, 'Noticias y Política (News & Politics)'),
        (26, 'Cómo Hacer (Howto & Style)'),
        (27, 'Educación (Education)'),
        (28, 'Ciencia y Tecnología (Science & Technology)'),
        (29, 'ONG y Activismo (Nonprofits & Activism)')
       
    ) AS datos(id, category_name)
)

SELECT * FROM categories