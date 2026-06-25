# Guía de calidad de datos

Los eventos StatsBomb pueden tener campos nulos según el tipo de acción. Algunos eventos no tienen jugador, coordenadas, xG u OBV. Por eso el pipeline limpia campos críticos y mantiene valores auxiliares para evitar errores en agregaciones.

Las alineaciones se extraen de eventos Starting XI. Si un partido no incluye esos eventos, la sección de alineaciones puede quedar vacía aunque existan eventos de juego. El informe debe diferenciar ausencia de dato y bajo rendimiento.

Los datos estáticos enriquecen equipos y jugadores si los nombres coinciden con StatsBomb. Si no coinciden exactamente, el enriquecimiento puede quedar como unknown sin afectar a las métricas principales calculadas desde eventos.
