// ============================================
// PySpark to Databricks — Shared Scripts
// ============================================

document.addEventListener('DOMContentLoaded', () => {
  initSidebar();
  initProgress();
  initQuizzes();
  initScrollSpy();
  highlightCurrentChapter();
});

// ============ Sidebar Toggle ============
function initSidebar() {
  const toggle = document.querySelector('.sidebar-toggle');
  const sidebar = document.querySelector('.sidebar');

  if (!toggle || !sidebar) return;

  toggle.addEventListener('click', () => {
    sidebar.classList.toggle('open');
    toggle.textContent = sidebar.classList.contains('open') ? '✕' : '☰';
  });

  // Close sidebar when clicking a link (mobile)
  sidebar.querySelectorAll('a').forEach(link => {
    link.addEventListener('click', () => {
      if (window.innerWidth < 1200) {
        sidebar.classList.remove('open');
        toggle.textContent = '☰';
      }
    });
  });
}

// ============ Reading Progress Bar ============
function initProgress() {
  const bar = document.querySelector('.progress-bar');
  if (!bar) return;

  window.addEventListener('scroll', () => {
    const scrollTop = window.scrollY;
    const docHeight = document.documentElement.scrollHeight - window.innerHeight;
    const progress = docHeight > 0 ? (scrollTop / docHeight) * 100 : 0;
    bar.style.width = Math.min(progress, 100) + '%';
  });
}

// ============ Quiz Functionality ============
function initQuizzes() {
  document.querySelectorAll('.quiz').forEach(quiz => {
    const options = quiz.querySelectorAll('.quiz-options li');
    const btn = quiz.querySelector('.quiz-btn');
    const answer = quiz.querySelector('.quiz-answer');
    const correctIndex = parseInt(quiz.dataset.correct, 10);
    let selected = null;

    options.forEach((opt, i) => {
      opt.addEventListener('click', () => {
        options.forEach(o => o.classList.remove('selected'));
        opt.classList.add('selected');
        selected = i;
      });
    });

    if (btn) {
      btn.addEventListener('click', () => {
        if (selected === null) return;

        options.forEach((opt, i) => {
          opt.classList.remove('selected');
          opt.style.pointerEvents = 'none';
          if (i === correctIndex) {
            opt.classList.add('correct');
          } else if (i === selected && i !== correctIndex) {
            opt.classList.add('incorrect');
          }
        });

        if (answer) {
          answer.classList.add('visible');
        }

        btn.disabled = true;
        btn.style.opacity = '0.5';
      });
    }
  });
}

// ============ Scroll Spy for Headings ============
function initScrollSpy() {
  const headings = document.querySelectorAll('h2[id], h3[id]');
  if (headings.length === 0) return;

  const observer = new IntersectionObserver(
    entries => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          const id = entry.target.id;
          document.querySelectorAll('.sidebar a').forEach(a => {
            a.classList.toggle('active',
              a.getAttribute('href') === '#' + id ||
              a.getAttribute('href')?.endsWith('#' + id)
            );
          });
        }
      });
    },
    { rootMargin: '-80px 0px -70% 0px' }
  );

  headings.forEach(h => observer.observe(h));
}

// ============ Highlight Current Chapter in Sidebar ============
function highlightCurrentChapter() {
  const currentPage = window.location.pathname.split('/').pop() || 'index.html';
  document.querySelectorAll('.sidebar a').forEach(a => {
    const href = a.getAttribute('href');
    if (href === currentPage || href === './' + currentPage) {
      a.classList.add('active');
    }
  });
}

// ============ Copy Code Button ============
document.addEventListener('click', e => {
  if (e.target.classList.contains('copy-btn')) {
    const pre = e.target.closest('pre') || e.target.parentElement.querySelector('pre');
    if (pre) {
      const code = pre.querySelector('code')?.textContent || pre.textContent;
      navigator.clipboard.writeText(code).then(() => {
        e.target.textContent = 'Copied!';
        setTimeout(() => { e.target.textContent = 'Copy'; }, 1500);
      });
    }
  }
});
