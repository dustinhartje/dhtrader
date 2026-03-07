// Apply unified styling to module docstrings by adding classes
document.addEventListener('DOMContentLoaded', function() {
    // Find all module sections
    const sections = document.querySelectorAll('section[id^="module-"]');
    
    sections.forEach(section => {
        // Find the first h1 (module heading)
        const h1 = section.querySelector('h1');
        if (!h1) return;
        
        // Find all p, ul, ol elements that are direct children
        const docElements = [];
        let currentElement = h1.nextElementSibling;
        
        // Collect elements until we hit a dl (API docs) or run out
        while (currentElement) {
            if (currentElement.tagName === 'DL') {
                break;
            }
            if (currentElement.tagName === 'P' || 
                currentElement.tagName === 'UL' || 
                currentElement.tagName === 'OL') {
                docElements.push(currentElement);
            }
            currentElement = currentElement.nextElementSibling;
        }
        
        // Add classes to identified docstring elements
        if (docElements.length > 0) {
            docElements.forEach(el => el.classList.add('module-docstring'));
            docElements[0].classList.add('module-docstring-first');
            docElements[docElements.length - 1].classList.add('module-docstring-last');
        }
    });
});
