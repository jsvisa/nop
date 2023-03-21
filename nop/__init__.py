import pkgutil
import inspect
from nop.extractor.extractor import NopExtractor


# load classes subclass of NopExtractor
platforms = []
for loader, name, is_pkg in pkgutil.walk_packages(__path__):
    # if not name.endswith("_orderbook_extractor"):
    #     continue
    module = loader.find_module(name).load_module(name)  # type: ignore
    for name, value in inspect.getmembers(module):
        if (
            inspect.isclass(value)
            and issubclass(value, NopExtractor)
            and value is not NopExtractor
            and not getattr(value, "ignore", False)
        ):
            globals()[name] = value
            platforms.append(value)
