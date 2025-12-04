\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{ULBreport}
\LoadClass[a4paper,11pt, openany]{report}

%%%%Structure%%%%
\RequirePackage[utf8]{inputenc}
\RequirePackage[T1]{fontenc}

\RequirePackage{tabularx}
\RequirePackage[top=2cm,bottom=2cm,left=2cm,right=2cm]{geometry}
\RequirePackage{hyperref}
\RequirePackage{enumitem}
\RequirePackage{xkeyval}
  
%Picture package
\RequirePackage{listings}
\RequirePackage{etoolbox}
\RequirePackage{setspace}
\RequirePackage{ccaption}
\RequirePackage{colortbl}
\RequirePackage{framed}
\RequirePackage{xcolor}
\RequirePackage{fourier}
\RequirePackage[pages=some]{background}

%Remove Chapter word on titles
\RequirePackage{titlesec}
\titleformat{\chapter}[block]{\normalfont\huge\bfseries}{\thechapter.}{1em}{}
\titleformat{\chapter}[block]{\normalfont\huge\bfseries}{}{0pt}{}

\makeatletter

% --- Suppress chapter number in section numbering ---
\renewcommand\thesection{\arabic{section}}
\renewcommand\thesubsection{\arabic{section}.\arabic{subsection}}
\renewcommand\thesubsubsection{\arabic{section}.\arabic{subsection}.\arabic{subsubsection}}
% ---------------------------------------------------------------------

% \patchcmd{\chapter}{%
%   \if@openright%
%     \cleardoublepage%
%   \else%
%     \clearpage%
%   \fi%
% }{\par}{}{}

% Number left side chapter's title
\titleformat{\chapter}[block]{\normalfont\huge\bfseries}{}{0pt}{}
\titleformat{\chapter}[hang]
{\normalfont\huge\bfseries}
{\thechapter}
{1em}
{}  

\titlespacing*{\chapter}
{0pt}                % Marge à gauche du titre
{0pt}                % Espace vertical AVANT le titre
{0pt}                % Espace vertical APRÈS le titre

%Keys for title page
\define@cmdkey [PRE] {fam} {title}{}
\define@cmdkey [PRE] {fam} {studies}{}
\define@cmdkey [PRE] {fam} {course}{}
\define@cmdkey [PRE] {fam} {author}{}
\define@cmdkey [PRE] {fam} {teacher}{}
\define@cmdkey [PRE] {fam} {date}{}
\define@cmdkey [PRE] {fam} {logo}{}
\define@cmdkey [PRE] {fam} {manyAuthor}[1]{}
%Set default value for the keys (used if not specified)
\presetkeys         [PRE] {fam} {title=titre,
    studies=,
    course=,
    author=Auteurs,
    teacher=,
    date=,
    logo=7,
    manyAuthor=8,}{}

% Title command
\newcommand*{\titleULB}[1]{\setkeys[PRE]{fam}{#1}

\begingroup
	\BgThispage
	% Replace logo with nothing if not specified
	\ifthenelse{\equal{\cmdPRE@fam@logo}{7}}{}{\includegraphics[height=2.2cm,width=20cm,keepaspectratio]{\cmdPRE@fam@logo}\\}
		{\cmdPRE@fam@studies}
	\begin{center}
	\vspace*{\baselineskip}
	{\bfseries \cmdPRE@fam@course}\\[\baselineskip]
	\rule{\textwidth}{1.6pt}\vspace*{-\baselineskip}\vspace*{2pt}
	\rule{\textwidth}{0.4pt}\\[\baselineskip]
	{\Huge \cmdPRE@fam@title}\\[\baselineskip]
	\rule{\textwidth}{0.4pt}\vspace*{-\baselineskip}\vspace{3.2pt}
	\rule{\textwidth}{1.6pt}\\[\baselineskip]
	\ifthenelse{\equal{\cmdPRE@fam@manyAuthor}{8}}{
	{\Large \cmdPRE@fam@author}\\[\baselineskip]
	{\bfseries \cmdPRE@fam@teacher}}{
	    \begin{minipage}{0.4\textwidth}
	        \begin{flushleft} \large
	        \cmdPRE@fam@author
	        \end{flushleft}
	    \end{minipage}
	    \begin{minipage}{0.4\textwidth}
	        \begin{flushright} \large
	        \cmdPRE@fam@teacher
	        \end{flushright}
	    \end{minipage}
	}
	\vfill
	{\cmdPRE@fam@date}
	\end{center}
	\vspace*{3\baselineskip}
	\pagenumbering{gobble}
	\newpage
        \renewcommand{\contentsname}{\fontsize{22pt}{24pt}\selectfont Glossary\\[20pt]}
        \setcounter{tocdepth}{4}
        \setcounter{secnumdepth}{4}	
        \tableofcontents
        \clearpage
        \counterwithout{figure}{chapter}
        \addtocontents{toc}{\vskip-40pt}
        \addtocontents{lof}{\vskip-40pt}
    \newpage
    \pagenumbering{arabic}
\endgroup}

\makeatother


%Command to put the seal of ULB in Background of the first page
\newcommand{\sceau}[1]{\backgroundsetup{
    scale=1,
    color=black,
    opacity=0.05,
    angle=10,
    position={12cm,-22cm},
    contents={%
    \includegraphics[height=20cm,width=20cm,keepaspectratio]{#1}
    }%
}}

\newcounter{boxlblcounter}  
\newcommand{\makeboxlabel}[1]{\fbox{#1.}\hfill}% \hfill fills the label box
\newenvironment{boxlabel}
  {\begin{list}
    {\arabic{boxlblcounter}}
    {\usecounter{boxlblcounter}
     \setlength{\labelwidth}{3em}
     \setlength{\labelsep}{0em}
     \setlength{\itemsep}{2pt}
     \setlength{\leftmargin}{1.5cm}
     \setlength{\rightmargin}{2cm}
     \setlength{\itemindent}{0em} 
     \let\makelabel=\makeboxlabel
    }
  }
{\end{list}}

\lstset{
    basicstyle=\ttfamily,
    breaklines=true,
}

\renewcommand{\theequation}{\arabic{equation}}

% %Add small heading in the left corner of the page.
% \usepackage{fancyhdr}
% \pagestyle{fancy}
% \fancyhf{}
% \rhead{Roberto Acquaviva, 2021}
% \lhead{MECA-H417 - Sustainable Energy}
% \usepackage{lastpage}
% \rfoot{\thepage/\pageref{LastPage}}


% end: excerpt from `alphabetic.bbx'