from io import BytesIO
from typing import Iterable

import numpy as np
from PIL import Image


def image_tensor_to_bytes(images: Iterable) -> bytes:
    """Serialize the first image tensor to PNG bytes."""
    image_list = list(images)
    if not image_list:
        raise ValueError("No images provided")
    tensor = image_list[0]
    array = _to_numpy(tensor)
    image = Image.fromarray(array)
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _to_numpy(tensor) -> np.ndarray:
    if hasattr(tensor, "cpu"):
        tensor = tensor.cpu()
    if hasattr(tensor, "numpy"):
        array = tensor.numpy()
    else:
        array = np.asarray(tensor)
    if array.ndim == 4:
        array = array[0]
    if array.shape[-1] == 1:
        array = np.repeat(array, 3, axis=-1)
    array = (array * 255).clip(0, 255).astype(np.uint8)
    return array

