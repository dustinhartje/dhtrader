// Make Table of Contents collapsible
// Collapses class methods by default, click class name to expand/collapse

document.addEventListener('DOMContentLoaded', function() {
    // Find the local TOC div
    var tocDiv = document.querySelector('.sphinxsidebarwrapper');
    if (!tocDiv) return;
    
    // Find all TOC list items
    var tocItems = tocDiv.querySelectorAll('ul li');
    
    tocItems.forEach(function(item) {
        var link = item.querySelector('a');
        if (!link) return;
        
        // Check if this item has nested ul (has children)
        var nestedUl = item.querySelector('ul');
        if (!nestedUl) return;
        
        // Check if this looks like a class (has methods nested under it)
        // Classes typically have code elements and nested lists
        var codeElement = link.querySelector('code');
        if (!codeElement) return;
        
        var linkText = codeElement.textContent;
        
        // If it looks like a class (capital letter, no parentheses at end)
        // then make it collapsible
        if (linkText && /^[A-Z]/.test(linkText) && !linkText.endsWith('()')) {
            // Start collapsed
            nestedUl.style.display = 'none';
            
            // Add a toggle indicator
            var indicator = document.createElement('span');
            indicator.textContent = ' ▸ ';
            indicator.style.cursor = 'pointer';
            indicator.style.userSelect = 'none';
            link.insertBefore(indicator, link.firstChild);
            
            // Make the whole link clickable to toggle
            link.style.cursor = 'pointer';
            link.addEventListener('click', function(e) {
                // Don't navigate, just toggle
                e.preventDefault();
                
                if (nestedUl.style.display === 'none') {
                    nestedUl.style.display = 'block';
                    indicator.textContent = ' ▾ ';
                } else {
                    nestedUl.style.display = 'none';
                    indicator.textContent = ' ▸ ';
                }
            });
        }
    });
});
