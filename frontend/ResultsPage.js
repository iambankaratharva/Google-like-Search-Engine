function onImageLoad() {
  //console.log("Image has loaded!");
  document.body.style.backgroundColor = "#f0f0f0";
}

document.addEventListener("DOMContentLoaded", function() {
    var urlParams = new URLSearchParams(window.location.search);
    var query = urlParams.get("query");
    var searchInput = document.getElementById("searchInput");
    if (searchInput && query) {
        searchInput.value = query;
    }
});

function correctCommonJsonErrors(text) {
    // Find the index of the last closing brace '}'
    const lastClosingBraceIndex = text.lastIndexOf('}');

    // Prune the text to the last closing brace, if it exists
    let correctedText = lastClosingBraceIndex !== -1 ? text.substring(0, lastClosingBraceIndex + 1) : text;

    // Check and correct for any JSON structure issues, such as unmatched brackets
    correctedText = balanceJsonBrackets(correctedText);

    return correctedText;
}

function balanceJsonBrackets(text) {
    const stack = [];
    let result = '';

    // Iterate through the text to check for balancing of brackets
    for (let i = 0; i < text.length; i++) {
        const char = text[i];
        if (char === '{' || char === '[') {
            stack.push(char);
            result += char;
        } else if ((char === '}' && stack[stack.length - 1] === '{') ||
                   (char === ']' && stack[stack.length - 1] === '[')) {
            stack.pop();
            result += char;
        } else {
            result += char;
        }
    }

    // Add missing closing brackets
    while (stack.length > 0) {
        const lastOpen = stack.pop();
        if (lastOpen === '{') {
            result += '}';
        } else if (lastOpen === '[') {
            result += ']';
        }
    }

    return result;
}

document.addEventListener("DOMContentLoaded", function() {
    const query = localStorage.getItem("searchQuery");
    localStorage.removeItem("searchQuery");

    if (query) {
        fetch(`/search?query=${encodeURIComponent(query)}`)
            .then((response) => {
                if (!response.ok) {
                    throw new Error(`HTTP error! Status: ${response.status}`);
                }
                return response.text();  // Get the raw text of the response
            })
            .then((text) => {
                const correctedText = correctCommonJsonErrors(text);
                try {
                    const data = JSON.parse(correctedText);
                    //console.log(`Data from /search: ${JSON.stringify(data)}`);
                    displaySearchResults(data);
                } catch (error) {
                    console.error("Error parsing JSON:", error);
                    //console.error("Corrected text that failed to parse:", correctedText);
                }
            })
            .catch((error) => {
                console.error("Error fetching the search results:", error);
            });
    }
});

//document.addEventListener("DOMContentLoaded", function () {
//
//  const query = localStorage.getItem("searchQuery");
//  localStorage.removeItem("searchQuery");
//
//  if (query) {
//      fetch(`/search?query=${encodeURIComponent(query)}`)
//          .then((response) => {
//              if (!response.ok) {
//                  throw new Error(`HTTP error! Status: ${response.status}`);
//              }
//              if (!response.headers.get("Content-Type")?.includes("application/json")) {
//                 console.log("response is not json");  // Automatically parses JSON response
//              }
//              return response.text();  // Get the raw text of the response
//          })
//          .then((text) => {
//              console.log("Raw JSON text:", text);
//              try {
//                  JSON.parse(text);  // Try parsing the text as JSON
//                  const data = JSON.parse(text);
//                  console.log("is array : ", Array.isArray(data));
//                  if (Array.isArray(data)) {
//                      const lastEntry = data[data.length - 1];
//                      console.log('Last entry:', lastEntry);
//                  }
//                  console.log(`Data from /search: ${JSON.stringify(data)}`);
//                  displaySearchResults(data);
//              } catch (error) {
//                  console.error("Error parsing JSON:", error);
//              }
//          })
//          .catch((error) => {
//              console.error("Error fetching the search results:", error);
//          });
//  }
//
//});


document.addEventListener("DOMContentLoaded", function () {
  const searchForm = document.querySelector(".search-form");
  searchForm.addEventListener("submit", function (event) {
    event.preventDefault(); 
    performSearch();
  });
});

function performSearch() {
  const query = document.getElementById("searchInput").value;
  // fetch(`/search?query=${encodeURIComponent(query)}`)
  //   .then((response) => {
  //     if (!response.ok) {
  //       throw new Error(`HTTP error! status: ${response.status}`);
  //     }
  //     return response.json();
  //   })
  //   .then((data) => {
  //     // Use the displaySearchResults function to show the results
  //     displaySearchResults(data);
  //   })
  //   .catch((error) => {
  //     console.error("Error fetching the search results:", error);
  //   });
  fetch(`/search?query=${encodeURIComponent(query)}`)
  .then((response) => {
      if (!response.ok) {
          throw new Error(`HTTP error! Status: ${response.status}`);
      }
      return response.text();  // Get the raw text of the response
  })
  .then((text) => {
      const correctedText = correctCommonJsonErrors(text);
      try {
          const data = JSON.parse(correctedText);
          //console.log(`Data from /search: ${JSON.stringify(data)}`);
          displaySearchResults(data);
      } catch (error) {
          console.error("Error parsing JSON:", error);
          //console.error("Corrected text that failed to parse:", correctedText);
      }
  })
  .catch((error) => {
      console.error("Error fetching the search results:", error);
  });
}

function displaySearchResults(results) {
  const searchResults = document.getElementById("searchResults");
  if (searchResults) {
    if (results.length === 0) {
      searchResults.style.display = "none";
      return;
    }
    searchResults.style.display = "block";

    searchResults.innerHTML = ""; // Clear previous results

    results.forEach((result) => {
      //console.log(result);
      const resultItem = createResultItem(result);
      searchResults.appendChild(resultItem);
    });
  } else {
    // If searchResults is null, log an error or handle it appropriately
    console.error("The searchResults container was not found.");
  }
}

// Function to create a result item
function createResultItem(result) {
  const container = document.createElement("div");
  container.classList.add("resultItem");

  const title = document.createElement("h3");
  const link = document.createElement("a");
  link.href = result.url;
  link.textContent = result.title || result.url;
  title.appendChild(link);
  container.appendChild(title);

  const url = document.createElement("u");
  url.textContent = result.url;
  container.appendChild(url);

  const description = document.createElement("p");
  description.textContent = result.page;
  container.appendChild(description);

  return container;
}

// bind the performSearch function to the form submission event
document.addEventListener("DOMContentLoaded", function () {
  const searchForm = document.querySelector(".search-form");
  searchForm.addEventListener("submit", function (event) {
    event.preventDefault(); // prevent the form from submitting the traditional way
    performSearch();
  });
});

document.addEventListener("DOMContentLoaded", function () {
  const searchInput = document.getElementById("searchInput");
  const resultsContainer = document.getElementById("autocompleteResults");

  searchInput.addEventListener("input", function (e) {
    const inputValue = e.target.value;
    const suggestions = JSON.parse(localStorage.getItem("wordlist")) || [];

    resultsContainer.innerHTML = "";
    resultsContainer.style.display = "none";

    if (inputValue) {
      suggestions[0]
        .filter(function (item) {
          return item.toLowerCase().startsWith(inputValue.toLowerCase());
        })
        .slice(0, 10)
        .forEach(function (suggested) {
          const div = document.createElement("div");
          div.textContent = suggested;
          div.className = "suggestion";
          div.addEventListener("click", function () {
            searchInput.value = suggested; // update the search input with the selected term
            resultsContainer.style.display = "none"; // hide the suggestions
          });
          resultsContainer.appendChild(div);
        });

      if (resultsContainer.childElementCount > 0) {
        resultsContainer.style.display = "block";
      } else {
        const suggestion = spellcheck(inputValue, suggestions[0]);

        if (suggestion && levenshteinDistance(inputValue, suggestion) <= 2) {
          // Threshold of 2 edits
          console.log("Did you mean:", suggestion);

          const div = document.createElement("div");
          div.textContent = suggestion;
          div.className = "suggestion";
          div.addEventListener("click", function () {
            searchInput.value = suggestion;
            resultsContainer.style.display = "none";
          });
          resultsContainer.appendChild(div);
          resultsContainer.style.display = "block";
        }
      }
    } else {
      // search history
    }
  });

  // hide suggestions when clicking outside
  document.addEventListener("click", function (e) {
    if (e.target !== searchInput) {
      resultsContainer.style.display = "none";
    }
  });
});

document.addEventListener("DOMContentLoaded", function () {
  const searchButton = document.getElementById("searchButton");
  searchButton.addEventListener("mouseout", function () {
    searchButton.textContent = "Search";
  });
});

function levenshteinDistance(a, b) {
  const matrix = [];

  for (let i = 0; i <= b.length; i++) {
    matrix[i] = [i];
  }

  for (let j = 0; j <= a.length; j++) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) == a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          Math.min(matrix[i][j - 1] + 1, matrix[i - 1][j] + 1)
        );
      }
    }
  }

  return matrix[b.length][a.length];
}

function spellcheck(input, dictionary) {
  let minDistance = Infinity;
  let closestWord = "";

  for (const word of dictionary) {
    const distance = levenshteinDistance(input, word);
    if (distance < minDistance) {
      minDistance = distance;
      closestWord = word;
    }
  }

  return closestWord;
}
