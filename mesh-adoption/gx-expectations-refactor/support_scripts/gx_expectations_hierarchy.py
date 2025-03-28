import inspect
import great_expectations.expectations.core as core

def get_all_classes_from_module(module):
    """Trova tutte le classi nel modulo."""
    return {name: obj for name, obj in inspect.getmembers(module, inspect.isclass)}

def get_class_hierarchy_until_expectation(class_obj):
    """Restituisce la gerarchia delle classi fermandosi alla classe 'Expectation'."""
    hierarchy = []
    for base_class in inspect.getmro(class_obj):
        hierarchy.append(base_class.__name__)
        if base_class.__name__ == 'Expectation':  # Fermati quando arrivi alla classe 'Expectation'
            break
    return hierarchy

# Ottieni tutte le classi nel modulo gx.expectations.core
module_classes = get_all_classes_from_module(core)

# Creiamo una lista di array, uno per ogni classe, che mostra il percorso dalla base fino a 'Expectation'
class_hierarchies = []

for class_name, class_obj in module_classes.items():
    # Otteniamo la gerarchia fino a 'Expectation' di ciascuna classe
    hierarchy = get_class_hierarchy_until_expectation(class_obj)
    # Invertiamo l'ordine per avere 'Expectation' come primo elemento
    class_hierarchies.append(hierarchy[::-1])

# Creiamo una struttura ad albero per le classi
def build_class_tree(hierarchies):
    tree = {}
    for hierarchy in hierarchies:
        current = tree
        for class_name in hierarchy:
            current = current.setdefault(class_name, {})
    return tree

# Costruisci l'albero della gerarchia
class_tree = build_class_tree(class_hierarchies)

# Funzione per stampare l'albero in modo leggibile
def print_tree(tree, level=0):
    """Stampa la gerarchia delle classi in modo leggibile, con indentazione."""
    for class_name, subtree in tree.items():
        print("  " * level + class_name)
        if isinstance(subtree, dict):
            print_tree(subtree, level + 1)

# Visualizza la gerarchia come albero
print_tree(class_tree)
