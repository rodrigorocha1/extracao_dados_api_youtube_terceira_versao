from typing import List, NewType, Tuple

Videos = NewType('Videos', str)
Canais = NewType('Canais', str)

ListaVideos = NewType('ListaVideos', List[Videos])
ListaCanaisVideo = NewType('ListaCanaisVideo', List[Tuple[Canais, Videos]])
