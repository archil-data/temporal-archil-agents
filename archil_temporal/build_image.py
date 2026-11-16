"""Register a pushed OCI image with Archil and print its image ID.

    python -m archil_temporal.build_image ghcr.io/you/archil-agent:v1

Archil builds the image once and sandboxes created from the returned ID reuse
that build. Register each new tag once (for example in CI) and roll the ID out
with the worker.
"""

from __future__ import annotations

import os
import sys

from archil import Archil, RegistryAuth


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    auth = None
    if os.environ.get("REGISTRY_PASSWORD"):
        auth = RegistryAuth(username=os.environ["REGISTRY_USERNAME"], password=os.environ["REGISTRY_PASSWORD"])
    image = Archil().images.build(source=sys.argv[1], registry_auth=auth)
    print(f"ARCHIL_IMAGE_ID={image.image_id}")


if __name__ == "__main__":
    main()
