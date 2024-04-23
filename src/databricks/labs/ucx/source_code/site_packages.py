import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SitePackage:

    @staticmethod
    def parse(path: Path):
        with open(Path(path, "RECORD"), encoding="utf-8") as record_file:
            lines = record_file.readlines()
            files = [line.split(',')[0] for line in lines]
            modules = list(filter(lambda line: line.endswith(".py"), files))
        top_levels_path = Path(path, "top_level.txt")
        if top_levels_path.exists():
            with open(top_levels_path, encoding="utf-8") as top_levels_file:
                top_levels = [line.strip() for line in top_levels_file.readlines()]
        else:
            dir_name = path.name
            # strip extension
            dir_name = dir_name[: dir_name.rindex('.')]
            # strip version
            dir_name = dir_name[: dir_name.rindex('-')]
            top_levels = [dir_name]
        return SitePackage(path, top_levels, [Path(module) for module in modules])

    def __init__(self, dist_info_path: Path, top_levels: list[str], module_paths: list[Path]):
        self._dist_info_path = dist_info_path
        self._top_levels = top_levels
        self._module_paths = module_paths

    @property
    def top_levels(self) -> list[str]:
        return self._top_levels

    @property
    def module_paths(self) -> list[Path]:
        return [Path(self._dist_info_path.parent, path) for path in self._module_paths]


class SitePackages:

    @staticmethod
    def parse(site_packages_path: Path):
        dist_info_dirs = [dir for dir in os.listdir(site_packages_path) if dir.endswith(".dist-info")]
        packages = [SitePackage.parse(Path(site_packages_path, dist_info_dir)) for dist_info_dir in dist_info_dirs]
        return SitePackages(packages)

    def __init__(self, packages: list[SitePackage]):
        self._packages: dict[str, SitePackage] = {}
        for package in packages:
            for top_level in package.top_levels:
                self._packages[top_level] = package

    def __getitem__(self, item: str) -> SitePackage | None:
        return self._packages.get(item, None)
