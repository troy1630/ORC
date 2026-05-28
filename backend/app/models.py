from dataclasses import dataclass


@dataclass(slots=True)
class RegistryItem:
    path: str
    name: str
    item_id: str
    version: str
    role_or_category: str
    approval_required: bool | None = None
