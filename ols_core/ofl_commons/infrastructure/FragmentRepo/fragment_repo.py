from abc import ABC, abstractmethod

from ols.ofl_commons.infrastructure.FragmentRepo.fragment_domain import Fragment

class FragmentRepo(ABC):
    @abstractmethod
    def get_fragment(self) -> Fragment:
        pass